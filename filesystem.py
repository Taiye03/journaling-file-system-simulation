import struct
import disk
import json
import time
import os

INODE_BLOCK = 2
INODE_BLOCKS = 2 
DATA_BLOCK_BASE = 67
BLOCK_SIZE = 512
DIR_ENTRY = 0x1111
FILE_CHUNK = 0x2222
UNUSED_ENTRY = 0xFFFF
BITMAP_BLOCK = 1
MAX_DATA_BLOCKS = BLOCK_SIZE * 8
JOURNAL_BLOCK = 3

class Journal:
    def __init__(self, disk, log_file="journal.log"):
        self.disk = disk
        self.pending_operations = []
        self.transaction_id = 0
        self.log_file = log_file
        self.load_journal()

    def load_journal(self):
        data = self.disk.readBlock(JOURNAL_BLOCK)
        journal_str = data.decode('utf-8', errors='ignore').rstrip('\x00')
        if journal_str:
            try:
                journal_data = json.loads(journal_str)
                self.pending_operations = journal_data.get('pending_operations', [])
                self.transaction_id = journal_data.get('transaction_id', 0)
            except (json.JSONDecodeError, UnicodeDecodeError):
                self.pending_operations = []
                self.transaction_id = 0
                self.save_journal()
        else:
            self.pending_operations = []
            self.transaction_id = 0

    def save_journal(self):
        journal_data = {
            'pending_operations': self.pending_operations,
            'transaction_id': self.transaction_id
        }
        journal_str = json.dumps(journal_data)
        journal_bytes = journal_str.encode('utf-8')
        if len(journal_bytes) > BLOCK_SIZE:
            journal_bytes = journal_bytes[:BLOCK_SIZE]
        else:
            journal_bytes += b'\x00' * (BLOCK_SIZE - len(journal_bytes))
        self.disk.writeBlock(JOURNAL_BLOCK, journal_bytes)

    def prepare_operation(self, operation_type, operation_data):
        self.transaction_id += 1
        operation = {
            'id': self.transaction_id,
            'type': operation_type,
            'data': operation_data,
            'timestamp': time.time(),
            'status': 'prepared'
        }
        self.pending_operations.append(operation)
        self.save_journal()
        return self.transaction_id

    def commit_operation(self, transaction_id):
        for op in self.pending_operations:
            if op['id'] == transaction_id:
                op['status'] = 'committed'
                self.save_journal()
                self.cleanup_committed_operations()
                return True
        return False

    def rollback_operation(self, transaction_id):
        self.pending_operations = [op for op in self.pending_operations if op['id'] != transaction_id]
        self.save_journal()

    def cleanup_committed_operations(self):
        self.pending_operations = [op for op in self.pending_operations if op['status'] != 'committed']
        self.save_journal()

    def recover_from_crash(self):
        rollback_count = 0
        for op in self.pending_operations[:]:
            if op['status'] == 'prepared':
                self.rollback_operation(op['id'])
                rollback_count += 1
        
        self.pending_operations = []
        self.save_journal()
        return rollback_count

    def log_operation(self, operation_type, details=""):
        log_entry = f"{operation_type}"
        if details:
            log_entry += f" {details}"
        log_entry += "\n"
        
        try:
            with open(self.log_file, "a") as f:
                f.write(log_entry)
        except IOError:
            pass

class BlockAllocator:
    def __init__(self, disk):
        self.disk = disk
        self.load_bitmap()

    def load_bitmap(self):
        data = self.disk.readBlock(BITMAP_BLOCK)
        self.bitmap = bytearray(data)

    def save_bitmap(self):
        while len(self.bitmap) < BLOCK_SIZE:
            self.bitmap.append(0)
        self.disk.writeBlock(BITMAP_BLOCK, bytes(self.bitmap))

    def is_block_free(self, block_num):
        if block_num >= MAX_DATA_BLOCKS:
            return False
        byte_index = block_num // 8
        bit_index = block_num % 8
        if byte_index >= len(self.bitmap):
            return True
        return (self.bitmap[byte_index] & (1 << bit_index)) == 0

    def allocate_block(self):
        for block_num in range(MAX_DATA_BLOCKS):
            if self.is_block_free(block_num):
                return self.mark_block_used(block_num)
        return None

    def mark_block_used(self, block_num):
        if block_num >= MAX_DATA_BLOCKS:
            return block_num
        byte_index = block_num // 8
        bit_index = block_num % 8
        while byte_index >= len(self.bitmap):
            self.bitmap.append(0)
        self.bitmap[byte_index] |= (1 << bit_index)
        self.save_bitmap()
        return block_num

    def mark_block_free(self, block_num):
        if block_num >= MAX_DATA_BLOCKS:
            return
        byte_index = block_num // 8
        bit_index = block_num % 8
        if byte_index < len(self.bitmap):
            self.bitmap[byte_index] &= ~(1 << bit_index)
            self.save_bitmap()

class FileSystem:
    def __init__(self, disk_file):
        self.disk_file = disk_file
        self.disk = disk.Disk(disk_file, BLOCK_SIZE)
        self.allocator = BlockAllocator(self.disk)
        self.inodes = self.get_inodes()
        self.current_block = self._find_root_block()
        self.path_stack = []
        self.path_names = []
        self.journal = Journal(self.disk)
        self.journal.recover_from_crash()
        self._ensure_root_directory()

    def _find_root_block(self):
        for idx, inode in enumerate(self.inodes):
            if inode["type"] == DIR_ENTRY and inode["directblock"][0] != 0:
                return DATA_BLOCK_BASE + inode["directblock"][0]
        
        if len(self.inodes) > 1 and self.inodes[1]["type"] == DIR_ENTRY:
            root_inode = self.inodes[1]
            return DATA_BLOCK_BASE + root_inode["directblock"][0]
        else:
            for idx, inode in enumerate(self.inodes):
                if inode["type"] == DIR_ENTRY:
                    return DATA_BLOCK_BASE + inode["directblock"][0]
        
        return DATA_BLOCK_BASE

    def get_inodes(self):
        inodes = []
        for block_num in range(INODE_BLOCK, INODE_BLOCK + INODE_BLOCKS):
            data = self.disk.readBlock(block_num)
            for i in range(0, len(data), 16):
                chunk = data[i:i+16]
                if len(chunk) == 16:
                    unpacked = struct.unpack('<HHHHHHHH', chunk)
                    inode = {
                        "type": unpacked[0],
                        "links": unpacked[1],
                        "size": unpacked[2],
                        "directblock": (unpacked[3], unpacked[4], unpacked[5]),
                        "indirectblock": unpacked[6],
                        "reserved": unpacked[7]
                    }
                    inodes.append(inode)
        return inodes

    def save_inodes(self):
        inode_data = bytearray()
        for inode in self.inodes:
            packed = struct.pack('<HHHHHHHH',
                inode["type"], inode["links"], inode["size"],
                inode["directblock"][0], inode["directblock"][1], inode["directblock"][2],
                inode["indirectblock"], inode.get("reserved", 0))
            inode_data += packed
        
        for block_num in range(INODE_BLOCK, INODE_BLOCK + INODE_BLOCKS):
            block_offset = (block_num - INODE_BLOCK) * BLOCK_SIZE
            block_data = bytearray(BLOCK_SIZE)
            
            start_idx = block_offset
            end_idx = min(start_idx + BLOCK_SIZE, len(inode_data))
            if start_idx < len(inode_data):
                copy_length = min(BLOCK_SIZE, end_idx - start_idx)
                block_data[:copy_length] = inode_data[start_idx:start_idx + copy_length]
            
            self.disk.writeBlock(block_num, bytes(block_data))

    def get_current_directory(self):
        current_data = bytearray(self.disk.readBlock(self.current_block))
        entries = []
        
        for i in range(0, len(current_data), 16):
            chunk = current_data[i:i+16]
            if len(chunk) == 16:
                entry = struct.unpack('<HH12s', chunk)
                if entry[0] != UNUSED_ENTRY and entry[1] != 0:
                    filename = entry[2].decode('utf-8', errors='ignore').rstrip('\x00')
                    inode_idx = entry[1]
                    
                    if filename and inode_idx < len(self.inodes) and self.inodes[inode_idx]["type"] != 0:
                        entries.append({"type": entry[0], "inode": inode_idx, "filename": filename})
            
        return entries

    def add_dir_entry(self, directory_block, inode_idx, filename, entry_type):
        current_data = bytearray(self.disk.readBlock(directory_block))
        for i in range(0, len(current_data), 16):
            chunk = current_data[i:i+16]
            if len(chunk) == 16:
                entry = struct.unpack('<HH12s', chunk)
                if entry[0] == UNUSED_ENTRY:
                    filename_bytes = filename.encode('utf-8')[:12]
                    filename_bytes += b'\x00' * (12 - len(filename_bytes))
                    new_entry = struct.pack('<HH12s', entry_type, inode_idx, filename_bytes)
                    current_data[i:i+16] = new_entry
                    self.disk.writeBlock(directory_block, bytes(current_data))
                    return True
        return False

    def find_inode_by_name(self, name):
        entries = self.get_current_directory()
        for entry in entries:
            if entry["filename"] == name:
                inode_idx = entry["inode"]
                if inode_idx < len(self.inodes) and self.inodes[inode_idx]["type"] != 0:
                    return inode_idx
        return None

    def entry_exists(self, name):
        return self.find_inode_by_name(name) is not None

    def find_free_inode(self):
        for idx, inode in enumerate(self.inodes):
            if inode["type"] == 0:
                return idx
        return None

    def create_directory(self, name):
        if self.entry_exists(name):
            return False
        
        transaction_id = self.journal.prepare_operation('create_directory', {'name': name, 'current_block': self.current_block})
        
        free_inode_idx = self.find_free_inode()
        if free_inode_idx is None:
            self.journal.rollback_operation(transaction_id)
            return False
        
        new_block_num = self.allocator.allocate_block()
        if new_block_num is None:
            self.journal.rollback_operation(transaction_id)
            return False
        
        try:
            self.inodes[free_inode_idx] = {
                "type": DIR_ENTRY,
                "links": 1,
                "size": 0,
                "directblock": (new_block_num, 0, 0),
                "indirectblock": 0
            }
            
            empty_dir_data = bytearray([0xFF] * BLOCK_SIZE)
            for i in range(0, BLOCK_SIZE, 16):
                if i + 16 <= BLOCK_SIZE:
                    struct.pack_into('<HH12s', empty_dir_data, i, UNUSED_ENTRY, 0, b'\x00' * 12)
            
            new_block_addr = DATA_BLOCK_BASE + new_block_num
            self.disk.writeBlock(new_block_addr, bytes(empty_dir_data))
            
            if not self.add_dir_entry(self.current_block, free_inode_idx, name, DIR_ENTRY):
                self.inodes[free_inode_idx] = {"type": 0, "links": 0, "size": 0, "directblock": (0, 0, 0), "indirectblock": 0}
                self.allocator.mark_block_free(new_block_num)
                self.journal.rollback_operation(transaction_id)
                return False
            
            self.save_inodes()
            self.journal.commit_operation(transaction_id)
            self.journal.log_operation("mkdir", name)
            return True
            
        except Exception:
            self.inodes[free_inode_idx] = {"type": 0, "links": 0, "size": 0, "directblock": (0, 0, 0), "indirectblock": 0}
            self.allocator.mark_block_free(new_block_num)
            self.journal.rollback_operation(transaction_id)
            return False

    def create_file(self, filename, content=""):
        if self.entry_exists(filename):
            return False
        
        transaction_id = self.journal.prepare_operation('create_file', {'filename': filename, 'content': content})
        
        inode_idx = self.find_free_inode()
        if inode_idx is None:
            self.journal.rollback_operation(transaction_id)
            return False
        
        content_blocks = []
        allocated_blocks = []
        
        try:
            if content:
                content_bytes = content.encode('utf-8')
                bytes_written = 0
                
                while bytes_written < len(content_bytes):
                    block_num = self.allocator.allocate_block()
                    if block_num is None:

                        for block in allocated_blocks:
                            self.allocator.mark_block_free(block)
                        self.journal.rollback_operation(transaction_id)
                        return False
                    
                    allocated_blocks.append(block_num)
                    chunk = content_bytes[bytes_written:bytes_written + BLOCK_SIZE]
                    if len(chunk) < BLOCK_SIZE:
                        chunk += b'\x00' * (BLOCK_SIZE - len(chunk))
                    
                    block_addr = DATA_BLOCK_BASE + block_num
                    self.disk.writeBlock(block_addr, chunk)
                    content_blocks.append(block_num)
                    bytes_written += BLOCK_SIZE
                    
                    if len(content_blocks) >= 3:
                        break
            direct_blocks = content_blocks + [0] * (3 - len(content_blocks))
            self.inodes[inode_idx] = {
                "type": FILE_CHUNK,
                "links": 1,
                "size": len(content.encode('utf-8')) if content else 0,
                "directblock": tuple(direct_blocks[:3]),
                "indirectblock": 0
            }
            if not self.add_dir_entry(self.current_block, inode_idx, filename, FILE_CHUNK):

                self.inodes[inode_idx] = {"type": 0, "links": 0, "size": 0, "directblock": (0, 0, 0), "indirectblock": 0}
                for block in allocated_blocks:
                    self.allocator.mark_block_free(block)
                self.journal.rollback_operation(transaction_id)
                return False
            
            self.save_inodes()
            self.journal.commit_operation(transaction_id)
            if content:
                self.journal.log_operation("write", filename)
            else:
                self.journal.log_operation("touch", filename)
            return True
            
        except Exception:
            self.inodes[inode_idx] = {"type": 0, "links": 0, "size": 0, "directblock": (0, 0, 0), "indirectblock": 0}
            for block in allocated_blocks:
                self.allocator.mark_block_free(block)
            self.journal.rollback_operation(transaction_id)
            return False

    def delete_entry(self, name):
        transaction_id = self.journal.prepare_operation('delete_entry', {'name': name, 'current_block': self.current_block})
        
        inode_idx = self.find_inode_by_name(name)
        if inode_idx is None:
            self.journal.rollback_operation(transaction_id)
            return False
        
        inode = self.inodes[inode_idx]
        current_data = bytearray(self.disk.readBlock(self.current_block))
        for i in range(0, len(current_data), 16):
            chunk = current_data[i:i+16]
            if len(chunk) == 16:
                entry = struct.unpack('<HH12s', chunk)
                if entry[1] == inode_idx:
                    new_entry = struct.pack('<HH12s', UNUSED_ENTRY, 0, b'\x00' * 12)
                    current_data[i:i+16] = new_entry
                    break
        
        self.disk.writeBlock(self.current_block, bytes(current_data))
        

        self.inodes[inode_idx]["links"] -= 1
        if self.inodes[inode_idx]["links"] <= 0:
            for block_num in inode["directblock"]:
                if block_num > 0:
                    self.allocator.mark_block_free(block_num)
            
            if inode["indirectblock"] > 0:
                self.allocator.mark_block_free(inode["indirectblock"])
            self.inodes[inode_idx] = {"type": 0, "links": 0, "size": 0, "directblock": (0, 0, 0), "indirectblock": 0}
        
        self.save_inodes()
        self.journal.commit_operation(transaction_id)
        if inode["type"] == DIR_ENTRY:
            self.journal.log_operation("rmdir", name)
        else:
            self.journal.log_operation("del", name)
        return True

    def change_directory(self, name):
        if name == "..":
            if self.path_stack:
                self.current_block = self.path_stack.pop()
                self.path_names.pop()
            return True
        
        inode_idx = self.find_inode_by_name(name)
        if inode_idx is None:
            return False
        
        inode = self.inodes[inode_idx]
        if inode["type"] != DIR_ENTRY:
            return False
        
        self.path_stack.append(self.current_block)
        self.path_names.append(name)
        self.current_block = DATA_BLOCK_BASE + inode["directblock"][0]
        return True

    def write_to_file(self, filename, content):
        transaction_id = self.journal.prepare_operation('write_file', {'filename': filename, 'content': content})
        
        inode_idx = self.find_inode_by_name(filename)
        if inode_idx is None:
            self.journal.rollback_operation(transaction_id)
            return False
        
        inode = self.inodes[inode_idx]
        if inode["type"] != FILE_CHUNK:
            self.journal.rollback_operation(transaction_id)
            return False
        
        content_bytes = content.encode('utf-8')
        
        for block_num in inode["directblock"]:
            if block_num > 0:
                self.allocator.mark_block_free(block_num)
        
        content_blocks = []
        bytes_written = 0
        
        while bytes_written < len(content_bytes):
            block_num = self.allocator.allocate_block()
            if block_num is None:
                self.journal.rollback_operation(transaction_id)
                return False
            
            chunk = content_bytes[bytes_written:bytes_written + BLOCK_SIZE]
            if len(chunk) < BLOCK_SIZE:
                chunk += b'\x00' * (BLOCK_SIZE - len(chunk))
            
            block_addr = DATA_BLOCK_BASE + block_num
            self.disk.writeBlock(block_addr, chunk)
            content_blocks.append(block_num)
            bytes_written += BLOCK_SIZE
            
            if len(content_blocks) >= 3:
                break
        
        direct_blocks = content_blocks + [0] * (3 - len(content_blocks))
        self.inodes[inode_idx]["size"] = len(content_bytes)
        self.inodes[inode_idx]["directblock"] = tuple(direct_blocks[:3])
        
        self.save_inodes()
        self.journal.commit_operation(transaction_id)
        self.journal.log_operation("write", filename)
        return True

    def read_file(self, filename):
        inode_idx = self.find_inode_by_name(filename)
        if inode_idx is None:
            return None
        
        inode = self.inodes[inode_idx]
        if inode["type"] != FILE_CHUNK:
            return None
        
        content = b""
        for block_num in inode["directblock"]:
            if block_num > 0:
                block_addr = DATA_BLOCK_BASE + block_num
                block_data = self.disk.readBlock(block_addr)
                content += block_data
        
        content = content.rstrip(b'\x00')
        return content.decode('utf-8', errors='ignore')

    def copy_file(self, src_name, dst_name):
        inode_idx = self.find_inode_by_name(src_name)
        if inode_idx is None:
            return False
        
        inode = self.inodes[inode_idx]
        if inode["type"] != FILE_CHUNK:
            return False
        
        content = self.read_file(src_name)
        if content is None:
            return False
        
        return self.create_file(dst_name, content)

    def create_hard_link(self, src_name, link_name):

        if self.entry_exists(link_name):
            return False
        src_inode_idx = self.find_inode_by_name(src_name)
        if src_inode_idx is None:
            return False
        
        src_inode = self.inodes[src_inode_idx]
        if src_inode["type"] != FILE_CHUNK:
            return False
        
        transaction_id = self.journal.prepare_operation('create_hard_link', 
                                                       {'src_name': src_name, 'link_name': link_name})
        if not self.add_dir_entry(self.current_block, src_inode_idx, link_name, FILE_CHUNK):
            self.journal.rollback_operation(transaction_id)
            return False
        self.inodes[src_inode_idx]["links"] += 1
        self.save_inodes()
        
        self.journal.commit_operation(transaction_id)
        self.journal.log_operation("link", f"{link_name} {src_name}")
        return True

    def get_label(self):
        data = self.disk.readBlock(0)
        return data.decode('utf-8', errors='ignore').strip('\x00').strip()

    def get_journal_status(self):
        return {
            'pending_operations': len(self.journal.pending_operations),
            'operations': self.journal.pending_operations,
            'transaction_id': self.journal.transaction_id
        }

    def get_current_path(self):
        if not self.path_names:
            return "/"
        return "/" + "/".join(self.path_names)

    def _ensure_root_directory(self):
        """Ensure a proper root directory exists and is initialized"""

        root_inode_idx = None
        root_block_num = None
        for idx, inode in enumerate(self.inodes):
            if inode["type"] == DIR_ENTRY and inode["directblock"][0] != 0:
                root_inode_idx = idx
                root_block_num = inode["directblock"][0]
                break
        if root_inode_idx is None:
            root_inode_idx = 1 if len(self.inodes) > 1 and self.inodes[1]["type"] == 0 else self.find_free_inode()
            if root_inode_idx is None:
                return
            
            root_block_num = self.allocator.allocate_block()
            if root_block_num is None:
                return
            self.inodes[root_inode_idx] = {
                "type": DIR_ENTRY,
                "links": 1,
                "size": 0,
                "directblock": (root_block_num, 0, 0),
                "indirectblock": 0
            }
            root_block_addr = DATA_BLOCK_BASE + root_block_num
            empty_dir_data = bytearray([0xFF] * BLOCK_SIZE)
            for i in range(0, BLOCK_SIZE, 16):
                if i + 16 <= BLOCK_SIZE:
                    struct.pack_into('<HH12s', empty_dir_data, i, UNUSED_ENTRY, 0, b'\x00' * 12)
            
            self.disk.writeBlock(root_block_addr, bytes(empty_dir_data))
            self.save_inodes()
        self.current_block = DATA_BLOCK_BASE + root_block_num
