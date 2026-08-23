# Journaling File System Simulation

A simulated UNIX-style file system implemented in Python, built on top of a virtual disk image. It supports directories, files, hard links, and a transactional journal for tracking filesystem operations, along with an interactive shell for exploring the file system.

## Overview

Traditional filesystems without journaling (e.g. ext2) are vulnerable to partial writes, orphaned blocks, and incomplete operations if the system crashes mid-update. This project explores that problem by layering a transaction journal on top of a simple UNIX-style filesystem:

- **Disk layer** (`disk.py`) — a fixed-size, block-addressable virtual disk backed by a single flat file (`disk.img`), with block-level read/write and I/O statistics.
- **Filesystem logic** (`filesystem.py`) — inode management, a bitmap-based free-block allocator, directory operations, file operations, and a journal manager.
- **Shell interface** (`shell.py`) — a `bash`-like CLI (`ls`, `cd`, `mkdir`, `touch`, `write`, `read`, `rm`, `link`, `journal`, `stats`, etc.) that also tracks the current working directory.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                Shell interface (shell.py)            │
├─────────────────────────────────────────────────────┤
│                  Filesystem logic                    │
│ ┌────────────┬──────────────┬────────────┬─────────┐ │
│ │  Journal   │  Directory   │    File    │  Block  │ │
│ │  manager   │  operations  │ operations │ alloc.  │ │
│ │            │              │            │         │ │
│ │ - prepare/ │ - create     │ - read     │ - bitmap│ │
│ │   commit/  │ - delete     │ - write    │   track │ │
│ │   rollback │ - list       │ - copy     │ - alloc │ │
│ │            │ - navigate   │ - delete   │ - free  │ │
│ │            │ - search     │ - link     │ - check │ │
│ └────────────┴──────────────┴────────────┴─────────┘ │
├─────────────────────────────────────────────────────┤
│                 Disk layer (disk.py)                 │
└─────────────────────────────────────────────────────┘
```

## Disk Layout

| Block | Purpose |
|---|---|
| 0 | Superblock — disk label |
| 1 | Bitmap — tracks free/used data blocks |
| 2–3 | Inode table — file and directory metadata |
| 3 | Journal — pending/committed operations (shares the block with the inode table region in this implementation) |
| 67+ | Data blocks — file contents and directory entries |

Each block is 512 bytes.

## Inode Structure

Each inode is a fixed 16-byte record:

| Offset | Field | Size | Description |
|---|---|---|---|
| 0–1 | Type | 2B | `0x0000` unused · `0x1111` directory · `0x2222` file |
| 2–3 | Links | 2B | Hard link count |
| 4–5 | Size | 2B | File size in bytes |
| 6–11 | Direct blocks (×3) | 2B each | Data block numbers |
| 12–13 | Indirect block | 2B | Reserved / unused in current operations |
| 14–15 | Reserved | 2B | Padding |

Files use up to 3 direct blocks (1,536 bytes max per file); there is no indirection currently wired up for larger files.

## Block Allocation Bitmap

The bitmap tracks which data blocks are in use, one bit per block (bit 0 → block 0, bit 1 → block 1, …). A `1` means allocated, `0` means free. It's stored at block 1 and rewritten on every allocation or free.

## Directory Entries

Directories are stored as 16-byte entries (`entry type`, `inode index`, `12-byte filename`), scanned linearly for lookups, insertions, and deletions. Filenames are capped at 12 characters.

## Transaction Journal

Every mutating operation (`create_directory`, `create_file`, `delete_entry`, `write_to_file`, `create_hard_link`) is wrapped in a journal transaction, persisted to disk as JSON with a transaction ID, operation type, parameters, timestamp, and status:

1. **Prepare** — `prepare_operation` assigns a transaction ID, records the pending operation, and saves the journal to disk with status `"prepared"`.
2. **Commit** — once the filesystem call succeeds, `commit_operation` marks the transaction `"committed"` and prunes it from the journal.
3. **Rollback** — if the operation fails partway (e.g. no free inode, no free block, duplicate name), `rollback_operation` discards the transaction and the code unwinds any inodes/blocks it had already claimed.

A separate, human-readable `journal.log` file also records a plain-text history of completed operations (`mkdir`, `write`, `del`, `link`, etc.), independent of the transactional journal.

## Known Limitation: Prepare Phase Is Not Write-Deferred

The journal's "prepare" phase is meant to record intent *before* touching real filesystem state, so that a crash between prepare and commit leaves nothing to undo. In the current implementation, however, the actual disk writes (inode table, bitmap, directory block) happen **during** the prepared phase, not after commit — `commit_operation` only flips the journal entry's status and doesn't apply any changes itself, because the real work has already landed on disk.

Practically, this means:

- Data is written immediately and durably (nothing is buffered waiting for a commit), so isolated crashes between operations are fine.
- But if a crash happens *mid-operation* (after some writes but before `commit_operation` runs), `recover_from_crash` only deletes the orphaned `"prepared"` journal entry — it cannot undo the partial writes that already happened, since those changes were never staged separately from the "real" filesystem state.
- This can still produce the exact problems journaling is meant to prevent: an allocated block with no owner, or a directory entry pointing at a partially updated inode.

A fully correct two-phase implementation would defer the actual `writeBlock`/`save_inodes`/`save_bitmap` calls until `commit_operation`, so nothing touches real state during "prepare" — matching the two-phase commit model in name as well as behavior.

## Usage

```bash
python shell.py
```

This launches the interactive shell against `disk.img` (created automatically on first run). Example session:

```
/> mkdir docs
Directory 'docs' created successfully
/> cd docs
Changed to directory: /docs
/docs> touch notes.txt
File 'notes.txt' created successfully
/docs> write notes.txt Hello, file system!
/docs> read notes.txt
Hello, file system!
/docs> journal
Journal Status:
  Transaction ID: 3
  Pending Operations: 0
  No pending operations
```

### Available Commands

| Category | Commands |
|---|---|
| Directories | `ls`, `pwd`, `cd <dir>`, `mkdir <name>`, `rmdir <name>` |
| Files | `touch <file>`, `rm`/`del <name>`, `read <file>`, `write <file> <content>`, `copy <src> <dst>`, `link <src> <dst>` |
| System | `label`, `journal`, `log [n]`, `stats`, `help`, `exit`/`quit` |

## Libraries Used

- `struct` — packing/unpacking binary data for on-disk inodes and directory entries
- `json` — serializing journal entries
- `os` — filesystem access and the `journal.log` file

## Requirements

- Python 3 (standard library only — no external dependencies)

## Files

- `disk.py` — virtual block device
- `filesystem.py` — inode table, allocator, journal, and file system operations
- `shell.py` — interactive command-line shell
