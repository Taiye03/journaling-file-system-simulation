from filesystem import FileSystem, DIR_ENTRY, FILE_CHUNK

class FileSystemShell:
    def __init__(self, disk_file="disk.img"):
        self.fs = FileSystem(disk_file)
    
    def display_directory(self, entries):
        if not entries:
            print("Directory is empty")
            return
        
        dirs = []
        files = []
        
        for entry in entries:
            if entry["inode"] < len(self.fs.inodes):
                inode = self.fs.inodes[entry["inode"]]
                if inode["type"] == DIR_ENTRY:
                    dirs.append(entry)
                elif inode["type"] == FILE_CHUNK:
                    files.append((entry, inode))
        
        dirs.sort(key=lambda x: x["filename"])
        files.sort(key=lambda x: x[0]["filename"])
        
        total_items = len(dirs) + len(files)
        print(f"Directory contents ({total_items} items):")
        print("=" * 40)
        
        if dirs:
            print("Directories:")
            for entry in dirs:
                print(f"   {entry['filename']}/")
        
        if files:
            if dirs:
                print()
            print("Files:")
            for entry, inode in files:
                size_str = f"{inode['size']} bytes" if inode['size'] > 0 else "empty"
                link_str = f" (links: {inode['links']})" if inode['links'] > 1 else ""
                print(f"   {entry['filename']:<12} ({size_str}){link_str}")
        
        if not dirs and not files:
            print("   (empty)")
        print("=" * 40)
    
    def change_directory(self, target_dir):
        return self.fs.change_directory(target_dir)
    
    def show_journal_status(self):
        journal_status = self.fs.get_journal_status()
        if journal_status['pending_operations'] > 0:
            print(f"[Journal: {journal_status['pending_operations']} pending operations]")
        
    def run(self):
        print("Type 'help' for available commands")
        self.show_journal_status()
        print()
        
        while True:
            current_path = self.fs.get_current_path()
            user_input = input(f"{current_path}> ").strip()
            
            if not user_input:
                continue
            
            parts = user_input.split()
            cmd = parts[0].lower()
            
            if cmd not in ["journal", "help", "exit", "quit"]:
                self.show_journal_status()
            
            if cmd == "exit" or cmd == "quit":
                print("\nExiting file system shell...")
                print("Final disk statistics:")
                self.fs.disk.printStats()
                print("Goodbye!")
                break
            elif cmd == "ls":
                entries = self.fs.get_current_directory()
                self.display_directory(entries)
            elif cmd == "cd":
                if len(parts) >= 2:
                    if not self.change_directory(parts[1]):
                        print(f"Cannot change to directory '{parts[1]}' (not found or not a directory)")
                    else:
                        print(f"Changed to directory: {self.fs.get_current_path()}")
            elif cmd == "mkdir":
                if len(parts) >= 2:
                    if self.fs.entry_exists(parts[1]):
                        print(f"Cannot create directory '{parts[1]}' (already exists)")
                    elif self.fs.find_free_inode() is None:
                        print(f"Cannot create directory '{parts[1]}' (disk full - no free inodes)")
                    elif not self.fs.create_directory(parts[1]):
                        print(f"Cannot create directory '{parts[1]}' (creation failed)")
                    else:
                        print(f"Directory '{parts[1]}' created successfully")
            elif cmd == "rmdir":
                if len(parts) >= 2:
                    if not self.fs.delete_entry(parts[1]):
                        print(f"Cannot remove directory '{parts[1]}'")
            elif cmd == "touch":
                if len(parts) >= 2:
                    if self.fs.entry_exists(parts[1]):
                        print(f"Cannot create file '{parts[1]}' (already exists)")
                    elif self.fs.find_free_inode() is None:
                        print(f"Cannot create file '{parts[1]}' (disk full no free inodes)")
                    elif not self.fs.create_file(parts[1]):
                        print(f"Cannot create file '{parts[1]}' (creation failed)")
                    else:
                        print(f"File '{parts[1]}' created successfully")
            elif cmd == "rm" or cmd == "del":
                if len(parts) >= 2:
                    if not self.fs.delete_entry(parts[1]):
                        print(f"Cannot remove '{parts[1]}' (not found)")
                    else:
                        print(f"'{parts[1]}' removed successfully")
            elif cmd == "read":
                if len(parts) >= 2:
                    content = self.fs.read_file(parts[1])
                    if content is not None:
                        print(content)
                    else:
                        print(f"Cannot read file '{parts[1]}'")
            elif cmd == "pwd":
                print(self.fs.get_current_path())
            elif cmd == "label":
                label = self.fs.get_label()
                if label:
                    print(f"Disk label: {label}")
                else:
                    print("No disk label set")
            elif cmd == "journal":
                journal_status = self.fs.get_journal_status()
                print(f"Journal Status:")
                print(f"  Transaction ID: {journal_status['transaction_id']}")
                print(f"  Pending Operations: {journal_status['pending_operations']}")
                if journal_status['operations']:
                    print("  Operations:")
                    for op in journal_status['operations']:
                        print(f"    ID {op['id']}: {op['type']} - {op['status']}")
                else:
                    print("  No pending operations")
            elif cmd == "log":
                try:
                    with open("journal.log", "r") as f:
                        lines = f.readlines()
                        if len(parts) >= 2 and parts[1].isdigit():
                            num_lines = int(parts[1])
                            lines = lines[-num_lines:]
                        for line in lines:
                            print(line.rstrip())
                except FileNotFoundError:
                    print("No journal.log file found")
                except Exception as e:
                    print(f"Error reading journal.log: {e}")
            elif cmd == "stats":
                self.fs.disk.printStats()
            elif cmd == "write":
                if len(parts) >= 3:
                    content = " ".join(parts[2:])
                    if not self.fs.write_to_file(parts[1], content):
                        print(f"Cannot write to file '{parts[1]}'")
            elif cmd == "copy":
                if len(parts) >= 3:
                    if not self.fs.copy_file(parts[1], parts[2]):
                        print(f"Cannot copy '{parts[1]}' to '{parts[2]}'")
            elif cmd == "link":
                if len(parts) >= 3:
                    if not self.fs.create_hard_link(parts[1], parts[2]):
                        print(f"Cannot link '{parts[1]}' to '{parts[2]}'")
                    else:
                        print(f"Hard link created: '{parts[2]}' -> '{parts[1]}'")
            elif cmd == "help":
                print("Available Commands:")
                print("=" * 50)
                print("Directory Operations:")
                print("  ls              - List directory contents")
                print("  pwd             - Show current directory path")
                print("  cd <dir>        - Change to directory")
                print("  mkdir <name>    - Create directory")
                print("  rmdir <name>    - Remove directory")
                print()
                print("File Operations:")
                print("  touch <file>    - Create empty file")
                print("  rm <name>       - Remove file or directory")
                print("  del <name>      - Remove file or directory")
                print("  read <file>     - Read and display file contents")
                print("  write <file> <content> - Write content to file")
                print("  copy <src> <dst> - Copy file (creates independent copy)")
                print("  link <src> <dst> - Create hard link (same file, different name)")
                print()
                print("System Operations:")
                print("  label           - Show disk label/name")
                print("  journal         - Show journal status and operations")
                print("  log             - Show journal.log entries")
                print("  stats           - Show disk usage statistics")
                print("  help            - Show this help message")
                print("  exit/quit       - Exit the shell")
                print("=" * 50)
                print("Filenames can only be up to 12 characters")
            else:
                print(f"Unknown command: {cmd}")

def main():
    shell = FileSystemShell()
    shell.run()

if __name__ == "__main__":
    main()
