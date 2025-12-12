# Key/Value Store in Python: Log-Structured Design and Variants

Implementation of two simple key/value storage engines in Python, developed to explore how data can be organized on disk to achieve fast insertions and efficient key-based retrievals.  

The project investigates alternative storage layouts and compares their performance:
- a log-structured engine inspired by the Bitcask store design.
- a simpler filesystem-based variant.

Together, these implementations illustrate how append-only logging, in-memory indexing, tombstones, and compaction shape the behavior of a simple storage engine.

This repository contains:

- A log-structured key/value store (`LogStructuredKV`) optimized for sequential writes  
- A baseline filesystem-based key/value store (`FilePerKeyKV`)  
- Benchmark scripts for evaluating write, read, and delete performance  
- User interfaces via CLI, REPL, and interactive menu  
- Documentation of design decisions and on-disk layout

---

## Project Structure

```text
.
├── kvstore/
│   ├── log_kv.py          # Log-structured Bitcask-style key/value store
│   ├── file_kv.py         # Simple filesystem-based key/value store (one file per key)
│   └── __init__.py
├── main.py                # CLI commands, REPL, and interactive menu
├── benchmark_compare.py   # Benchmark comparing both storage engines
├── benchmark.py           # Benchmark focused on LogStructuredKV with configurable parameters
└── data/                  # Auto-created directory used for persistence during execution

```
---

## Architectural Overview

This project implements two alternative storage engines to explore how different on-disk layouts affect write, read, and delete performance.

### 1. LogStructuredKV (append-only log with in-memory index)

A simplified Bitcask-style storage engine. Each update is appended to a log file, while an in-memory hash index maps keys to offsets.



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


#### Record Layout

Each appended record uses a fixed binary format:

```text
+----------+----------+--------+-------------------+--------------------+
| key_len  | val_len  | tomb   |     key_bytes     |    value_bytes     |
|  4 bytes | 4 bytes  | 1 byte |    variable       |     variable       |
+----------+----------+--------+-------------------+--------------------+
      |        |         |             |                    |
      |        |         |             |                    +-- actual serialized value
      |        |         |             +------------------------- key as bytes
      |        |         +-- deletion flag (0=normal, 1=tombstone)
      |        +-- length of value in bytes
      +-- length of key in bytes
```

- `key_len` and `val_len` specify the sizes of the following byte sequences.  
- `tomb` marks deletion without removing older records.

#### Operation Flow

**PUT(key, value)**  
- Rotate `active.log` if size threshold is reached  
- Append header + key + value  
- Update the in-memory index  

**GET(key)**  
- Look up key in the hash index  
- Seek directly to the record location and read it  

**DELETE(key)**  
- Append a tombstone record  
- Remove the key from the index  

**COMPACT()**  
- Rewrite only the latest live values into a new segment  
- Drop tombstones and obsolete records  
- Remove older segments and rebuild the index  

#### Characteristics

- Sequential writes ensure predictable throughput  
- Single disk seek per read  
- In-memory index provides constant-time lookups  
- Tombstones allow deletions without modifying older files  
- Compaction reclaims space and merges segment files  
- All keys must fit into memory (index requirement) 

---

### 2. FilePerKeyKV (one file per key, in-place updates)

This simpler variant maps each key directly to a file in the filesystem.

```text
data/fs/
├── key_00001
├── key_00002
├── key_00003
└── ...
```

#### Operation Flow

**SET(key, value)**  
- Convert the key into a file path inside the storage directory  
- Overwrite the file `<data_dir>/<key>` with the new value  
- No append-only semantics, no segments, no indexing  

**GET(key)**  
- Open `<data_dir>/<key>` and return its contents  
- If the file does not exist, return `None`  

**DELETE(key)**  
- Remove the corresponding file from disk  

#### Characteristics

- Very simple persistence model  
- Writes rely on random I/O due to file overwriting  
- Deletes physically remove files from the filesystem  
- No compaction step (the filesystem handles space reclamation)  
- No in-memory index; lookup cost depends on filesystem metadata access  
- Useful as a baseline to highlight the benefits of log-structured designs  

#### Contrasting with LogStructuredKV

- No append-only writes  
- No tombstones  
- No segment rotation  
- No compaction  
- No key-to-offset mapping  

This variant helps illustrate how storage layout decisions influence performance, especially under workloads with many writes or frequent updates.

---

## Implementation Summary

### LogStructuredKV (log-structured engine)

| Aspect              |  Technique Used                                  |
|---------------------|--------------------------------------------------|
| Data storage        | Append-only log segments                         |
| Record structure    | Length-prefixed binary format with tombstone flag|
| Read performance    | In-memory hash index: key → (file, offset)       |
| Deletes             | Tombstone records appended to the log            |
| Data reclamation    | Compaction (segment merging and rewriting  data) |
| Crash recovery      | Rebuild index by scanning all log segments       |

### FilePerKeyKV (filesystem-based variant)

| Aspect               | Technique Used                                  |
|---------------------|--------------------------------------------------|
| Data storage        | One file per key                                 |
| Writes              | In-place file overwrite                          |
| Reads               | Direct filesystem lookup and file read           |
| Deletes             | File removal                                     |
| Data reclamation    | Handled implicitly by filesystem                 |
| Indexing            | None (directory structure only)                  |

---

## Usage

Clone the repository:

```bash
git clone https://github.com/amulyabhandari/DIPKeyValueStore.git
cd DIPKeyValueStore
```

### Menu Mode

Running the program without arguments starts an interactive menu where keys can be added, viewed, deleted, and compacted:

```bash
python main.py
```

### Command-Line Interface

One-shot operations can be executed directly:
```bash
python main.py set <key> <value>
python main.py get <key>
python main.py delete <key>
python main.py compact
```
Use a custom storage directory if needed:

```bash
python main.py --data-dir ./mydata set key value
```

### REPL Mode
```bash
python main.py repl
```
Supported commands:
set <key> <value>
get <key>
delete <key>
compact
exit

---

## Benchmarking

Two benchmark tools are provided to evaluate storage performance under different workloads.

### Benchmarking LogStructuredKV Alone

A configurable benchmark is available to measure the raw performance of the log-structured engine:

```bash
python benchmark.py --num-ops 20000 --value-size 100
```
Arguments:

- `--num-ops` : number of SET / GET / DELETE operations (default: 20000)
- `--value-size` : size of randomly generated values in bytes (default: 100)
- `--data-dir` : directory used to store benchmark log files

This benchmark isolates the behavior of the append-only log design, making it easier to evaluate how record layout, indexing strategy, and compaction impact throughput.

### Comparing Both Storage Engines

A second benchmark script compares the log-structured engine with the simpler filesystem-based variant:

```bash
python benchmark_compare.py
```
It measures the throughput of PUT, GET, and DELETE operations for each engine, then reports relative slowdowns or speedups.

---

## Authors and Project Context

This project was developed as part of the **Master 2 in Data Science** at **Polytech Nantes, Nantes Université**, within the course **Data-Intensive Processing**.

- **Amulya Bhandari** — https://github.com/amulyabhandari
- **Emiliano Pavicich** — https://github.com/epavicich  

---

## References

- Martin Kleppmann, *Designing Data-Intensive Applications*, O’Reilly Media, 2017.  
  (Record formats, log-structured storage, compaction, and indexing techniques referenced in Chapter 3.)
- Guillaume Raschia, *Data-Intensive Processing*, course lectures, Polytech Nantes — Nantes Université, 2025.
