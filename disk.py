import os

class Disk:

    DEFAULT_NUM_BLOCKS = 1024

    def __init__(self, disk_file: str, block_size: int):
        self.disk_file = disk_file
        self.block_size = block_size
        self.num_blocks = self.DEFAULT_NUM_BLOCKS
        self.reads = 0
        self.writes = 0

        if not os.path.exists(disk_file):
            self._initialize_disk()

    def _initialize_disk(self):

        import struct
        data = bytearray(b'\x00' * self.block_size * self.num_blocks)

        # Inode table starts at block 2; each inode is 16 bytes.
        # Write inode 0 with type=0x0001 (reserved — not DIR_ENTRY or FILE_CHUNK).
        INODE_TABLE_OFFSET = 2 * self.block_size
        RESERVED_TYPE = 0x0001
        struct.pack_into('<H', data, INODE_TABLE_OFFSET, RESERVED_TYPE)

        with open(self.disk_file, 'wb') as f:
            f.write(bytes(data))

    def _block_offset(self, block_num: int) -> int:
        return block_num * self.block_size

    def readBlock(self, block_num: int) -> bytes:
        """
        Read one block from disk.

        Args:
            block_num: Zero-based block index.

        Returns:
            bytes of length block_size. Returns a zero-filled block if the
            block is beyond the end of the file.
        """
        if block_num < 0:
            raise ValueError(f"Invalid block number: {block_num}")

        self.reads += 1

        try:
            with open(self.disk_file, 'rb') as f:
                f.seek(self._block_offset(block_num))
                data = f.read(self.block_size)
        except FileNotFoundError:
            return b'\x00' * self.block_size

        # Pad with zeros if the read returned fewer bytes than expected
        if len(data) < self.block_size:
            data += b'\x00' * (self.block_size - len(data))

        return data

    def writeBlock(self, block_num: int, data: bytes):
        """
        Write one block to disk.

        Args:
            block_num: Zero-based block index.
            data: Exactly block_size bytes to write. Truncated or padded
                  with zeros if the length does not match.
        """
        if block_num < 0:
            raise ValueError(f"Invalid block number: {block_num}")

        # Enforce exact block size
        if len(data) < self.block_size:
            data = data + b'\x00' * (self.block_size - len(data))
        elif len(data) > self.block_size:
            data = data[:self.block_size]

        self.writes += 1

        # Extend the file if writing past the current end
        file_size = os.path.getsize(self.disk_file) if os.path.exists(self.disk_file) else 0
        required_size = self._block_offset(block_num) + self.block_size

        with open(self.disk_file, 'r+b' if os.path.exists(self.disk_file) else 'w+b') as f:
            if required_size > file_size:
                f.seek(0, 2)  # Seek to end
                f.write(b'\x00' * (required_size - file_size))
            f.seek(self._block_offset(block_num))
            f.write(data)

    def printStats(self):
        """Print disk usage and I/O statistics."""
        try:
            file_size = os.path.getsize(self.disk_file)
            actual_blocks = file_size // self.block_size
        except FileNotFoundError:
            actual_blocks = 0
            file_size = 0

        print(f"Disk Statistics:")
        print(f"  Image file  : {self.disk_file}")
        print(f"  Block size  : {self.block_size} bytes")
        print(f"  Total blocks: {actual_blocks}")
        print(f"  Total size  : {file_size:,} bytes ({file_size / 1024:.1f} KB)")
        print(f"  Reads       : {self.reads}")
        print(f"  Writes      : {self.writes}")
