# DIPKeyValueStore

Educational implementation of two simple key/value storage engines in Python for the Data-Intensive Processing (DIP) course at Master 2 in Data Science (Polytech Nantes, Nantes Université).  
The project demonstrates how different storage layouts affect performance under write/read workloads.

This repository contains:

- A log-structured, Bitcask-style key/value store (`LogStructuredKV`)
- A baseline filesystem-based key/value store (`FilePerKeyKV`)
- A benchmark script comparing both designs
- User access through CLI, REPL, and interactive menu

---

## Project Structure
.
├── kvstore/
│ ├── log_kv.py # Log-structured Bitcask-style key/value store
│ ├── file_kv.py # Baseline filesystem store (one file per key)
│ └── init.py
├── main.py # CLI, REPL, interactive menu
├── benchmark_compare.py # Performance benchmark suite
└── data/ # Auto-created storage directory


---

## Architectural Overview

Two different storage architectures are implemented.

---

### 1. LogStructuredKV (append-only log with in-memory index)

A simplified Bitcask-style storage engine.

             +-----------------------------+
             |       LogStructuredKV       |
             +-----------------------------+
             |  In-memory hash index       |
             |  {key -> (file, offset)}    |
             +--------------+--------------+
                            |
                            v
    +--------------------------- data/ ---------------------------+
    |                                                               
    |   active.log         segment-00001.log    segment-00002.log   
    |   (current writes)   (older, immutable)   (older, immutable)  
    |                                                               
    +----------------------------------------------------------------

Record layout in log files:
[key_len:4][val_len:4][tomb:1][key_bytes][value_bytes]


Operation flow:

SET(key, value)

Rotate active.log if threshold reached

Append header + key + value into active.log

Update in-memory index

GET(key)

Look up key in index

Seek to file offset in correct segment

Decode and return value

DELETE(key)

Append tombstone record (tomb=1)

Remove key from index

COMPACT()

Write only latest live values into a new segment

Remove all old segments

Rebuild index from compacted file


Characteristics:

- Sequential writes
- Fast point lookups through in-memory index
- Tombstone-based deletes
- Full compaction step
- Crash recovery by scanning `.log` files at startup

---

### 2. FilePerKeyKV (one file per key, in-place updates)

A minimal baseline key/value store mapping each key to a single file.

data/fs/
├── key_00001
├── key_00002
├── key_00003
└── ...

Operation flow:

SET(key, value)

Overwrite <data_dir>/<key> directly

GET(key)

Read the file <data_dir>/<key>

DELETE(key)

Remove <data_dir>/<key>


Characteristics:

- Random in-place writes
- No logs, no compaction, no index
- Used only for baseline performance comparison

---

## Features

### LogStructuredKV

- Append-only logging and segment rotation
- Binary on-disk record format
- In-memory hash index for constant-time lookups
- Tombstones to represent deletions
- Full compaction that rewrites only latest values
- Crash recovery via log scanning

### FilePerKeyKV

- One file per key
- Simple overwrite-based writes
- Direct filesystem lookups
- Minimal implementation for comparison

### Benchmark Suite

- Measures PUT, GET, DELETE throughput
- Compares LogStructuredKV vs FilePerKeyKV
- Reports speedup factors

---

## Installation

Clone the repository:

git clone https://github.com/amulyabhandari/DIPKeyValueStore.git
cd DIPKeyValueStore

Usage
Menu Mode

Run without arguments:

python main.py

Interactive menu with options to add, view, delete, and compact data.

CLI Commands

python main.py set <key> <value>
python main.py get <key>
python main.py delete <key>
python main.py compact

Specify a custom data directory:
python main.py --data-dir ./mydata set key value

REPL Mode
python main.py repl

Supported commands:
set <key> <value>
get <key>
delete <key>
compact
exit


Benchmarking
Run the benchmark suite:
python benchmark_compare.py

The script performs PUT, GET, and DELETE operations and prints:

Time per phase

Throughput (operations/sec)

Speedup comparing LogStructuredKV vs FilePerKeyKV

Example output format:
PUT    : 0.45 sec, 44000 ops/sec
GET    : 0.30 sec, 66000 ops/sec
DELETE : 0.40 sec, 50000 ops/sec
TOTAL  : 1.15 sec, 52000 ops/sec

These results illustrate the impact of sequential writes and in-memory indexing.


