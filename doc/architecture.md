# bxdb Architecture

## 1. Overview

`bxdb` is a write-once, read-many versioned page store. It stores 4 KB memory
pages indexed by a physical address (PA) and a snapshot ID. Given a PA and a
snapshot ID, it returns the page as it existed at the most recent snapshot whose
ID is ≤ the requested ID (a **floor query**).

The system is built around a strict **phase separation**: a burst write phase is
followed by an offline conversion step, after which a read-only phase begins.
Reads and writes never overlap. Both the write and read interfaces are fully
**synchronous** — every call blocks until the operation is complete and durable.

---

## 2. Goals and Constraints

| Property | Requirement |
|---|---|
| Page size | Fixed 4 KB |
| Snapshot ID | At most 19 bits (max value 524287) |
| Physical address | At most 45 bits |
| Combined key | 64-bit integer: bits 63..19 = PA, bits 18..0 = snapshot_id |
| Write latency | Fast; parallelised internally across workers |
| Durability | `bxdb_save_pages` flushes to disk before returning |
| Read latency | Tolerant; targeting milliseconds (compute < 10% of I/O time) |
| Operations | Create and read only — no updates, no deletes |
| Interface | Compiled shared library with a synchronous C API |

### Combined Key Encoding

```
 63                  19 18              0
 ┌────────────────────┬─────────────────┐
 │     PA  (45 bits)  │ snapshot_id (19b)│
 └────────────────────┴─────────────────┘
```

This packing means a single `u64` uniquely identifies any `(PA, snapshot_id)`
pair within the supported ranges.

---

## 3. C API

```c
/* Initialise the library. Must be called once before any other function. */
bxdb_t *bxdb_init(void);

/* Open a database in fw (functional-warming) mode. Supports bulk saves
 * and bulk loads of pages. worker_count sets parallelism for saves.
 * delta_threshold: max non-zero words in a patch before a Full chunk is
 * written instead (default: 256, i.e. half of a 4 KB page). */
bxdb_t *bxdb_open_for_fw(const char *name, int worker_count,
                          uint16_t delta_threshold);

/* Open a database in timing mode (after conversion has been run).
 * Supports single-page reads only. */
bxdb_t *bxdb_open_for_timing(const char *name);

/*
 * Save pages for a snapshot. Synchronous — blocks until all dirty pages
 * have been compressed, written to disk, and fsynced.
 *
 * memory           - contiguous array of total_page_count × 4096 bytes
 * dirty_bitmap     - one bit per page; only pages with bit=1 are processed
 * total_page_count - number of pages in memory[]
 * snapshot_id      - must fit in 19 bits
 */
void bxdb_save_pages(bxdb_t         *db,
                     const char     *memory,
                     const uint64_t *dirty_bitmap,
                     uint64_t        total_page_count,
                     uint32_t        snapshot_id);

/*
 * Load a single page. Synchronous. Requires a timing-mode handle.
 *
 * Fills page[0..4095] with the content of PA at the largest stored
 * snapshot_id' ≤ snapshot_id. Returns false if no such page exists.
 */
bool bxdb_load_page(bxdb_t   *db,
                    char     *page,
                    uint64_t  pa,
                    uint32_t  snapshot_id);

/*
 * Load all pages for a snapshot into a contiguous buffer. Synchronous.
 * Requires an fw-mode handle.
 *
 * pages            - output buffer of total_page_count × 4096 bytes
 * pa_offset        - PA of the first page (pages[0] = PA pa_offset,
 *                    pages[1] = PA pa_offset+1, etc.)
 * total_page_count - number of pages to load
 * snapshot_id      - floor query applied to every page
 * worker_count     - degree of read parallelism
 *
 * Returns false if any page could not be resolved. Pages that have no
 * stored version are zero-filled.
 */
bool bxdb_load_all_pages(bxdb_t   *db,
                         char     *pages,
                         uint64_t  pa_offset,
                         uint64_t  total_page_count,
                         uint32_t  snapshot_id,
                         int       worker_count);
```

A separate binary `bxdb-convert` converts the write-phase output into the
B-tree read index (see §7).

---

## 4. Chunk Types

Every stored page is represented as one of three chunk types:

```rust
enum ChunkType {
    // XOR diff against a base version of the same PA.
    // patch: only the non-zero (word_index, xor_value) pairs after XOR.
    Delta {
        base: u64,               // combined key of the epoch base chunk
        patch: Vec<(u16, u64)>,  // (word index in 8-byte units, xor value)
    },

    // Full page compressed with zstd.
    Full {
        data: Vec<u8>,
    },

    // Page is entirely zero bytes. No blob data stored.
    Zero,
}
```

---

## 5. Adaptive Compression

Versions of the same PA form a two-level structure: a `Full` base chunk, and
`Delta` chunks that each diff directly against that base. A `Delta` never
points at another `Delta`, so reading any version costs at most **2 blob
reads**: the base plus the target delta.

### When a Full Chunk Is Created

A `Full` chunk is written for a given PA when **either** condition holds:

1. The PA has no prior stored version (first time it is seen by a worker).
2. The XOR delta against the current base contains more than 256 non-zero
   8-byte words — i.e., more than half of the 512 words in a 4 KB page. At
   this point the patch is larger than half a page and a fresh Full chunk is
   cheaper to store and equally fast to read.

When a new `Full` chunk is created under condition 2, it becomes the new base
for all subsequent deltas of that PA.

### Zero Detection

Before attempting delta or full compression, a page is checked for all-zero
content. If zero, a `Zero` chunk is recorded with no blob data.

---

## 6. Write Path

### Caller View

`bxdb_save_pages` is fully synchronous. The caller passes the snapshot memory
and dirty bitmap, and the function returns only after all dirty pages are
durable on disk.

### Internal Parallelism

Worker threads exist solely to **process dirty pages in parallel** within a
single `bxdb_save_pages` call. The main thread scans the dirty bitmap
sequentially and dispatches each set bit as a job to a shared work queue.
Worker threads pull jobs from the queue concurrently.

```
main thread:
  for i in 0..total_page_count:
    if dirty_bitmap[i] == 1:
      push job (pa = pa_base + i, page = memory + i*4096) to work queue

  signal end-of-jobs
  wait for all workers to finish
  fsync all blob files
  fsync chunks.log
  return
```

This approach requires no knowledge of how many dirty pages exist before
scanning begins, and load-balances naturally across workers.

### Per-Worker Processing

All workers share a single **global shadow** that maps each PA to its current
Full base page and the combined key of that base:

```rust
let shadow: RwLock<HashMap<u64, (u64, Box<[u8; 4096]>)>> = RwLock::new(HashMap::new());
//                          PA    base_key  base_page
```

A read lock is taken to look up the base when computing a delta. A write lock
is taken only when inserting a new Full chunk for a PA (either first occurrence
or threshold exceeded). Since Full chunks are rare relative to Delta chunks,
write-lock contention is low.

This avoids any per-worker duplication: if two workers happen to process
different snapshots of the same PA, they see the same base and agree on whether
a new Full is needed.

For each job a worker receives:

```
1. If page is all zeros → emit Zero chunk, skip remaining steps.
2. Read-lock shadow; look up shadow[pa]:
     Found (base_key, base_page):
       compute patch = xor(page, base_page), collect non-zero (word_index, value) pairs
       if non-zero word count ≤ delta_threshold:
         release read lock
         emit Delta { base: base_key, patch }
       else:
         release read lock; write-lock shadow
         emit Full chunk (zstd compress)
         shadow[pa] = (new_chunk_key, page)
         release write lock
     Not found:
       release read lock; write-lock shadow
       emit Full chunk (zstd compress)
       shadow[pa] = (new_chunk_key, page)
       release write lock
3. Append ChunkRecord to chunks.log
4. Write blob data to this worker's blob file
```

Each worker writes to its own blob file, eliminating contention:

```
<name>/blobs/worker_0.blob
<name>/blobs/worker_1.blob
...
```

---

## 7. On-Disk Format

### Magic Numbers

Every bxdb file begins with an 8-byte magic number. This allows `load_all_pages`
(and any tool) to identify the format without relying on file extensions or
external metadata.

| File | Magic bytes | ASCII |
|---|---|---|
| `chunks.log` (append-only) | `42 58 44 42 4C 4F 47 00` | `BXDBLOG\0` |
| `index.bxdb` (B-tree) | `42 58 44 42 49 44 58 00` | `BXDBIDX\0` |

Both files additionally store a 1-byte format version immediately after the
magic (currently `0x01`), allowing future format changes to be detected.

### Append-Only Format (`chunks.log`)

```
<name>/
├── chunks.log          ← magic header + append-only ChunkRecord sequence
└── blobs/
    ├── worker_0.blob
    ├── worker_1.blob
    └── ...
```

File layout:

```
┌─────────────────────────────────┐
│ magic (8 bytes): BXDBLOG\0      │
│ version (1 byte): 0x01          │
│ reserved (7 bytes): 0x00...     │
├─────────────────────────────────┤
│ ChunkRecord[]  (variable)       │
└─────────────────────────────────┘
```

Each `ChunkRecord`:

```
┌──────────┬────────┬─────────────┬───────────┬─────────┐
│ key (u64)│type(u8)│worker_id(u8)│offset (u64)│ len(u32)│
└──────────┴────────┴─────────────┴───────────┴─────────┘
```

For `Delta` chunks, the record is followed by `base_key (u64)`.
For `Zero` chunks, `offset` and `len` are zero.

`type`: `0` = Full, `1` = Delta, `2` = Zero.

### B-Tree Index Format (`index.bxdb`)

Produced by `bxdb-convert`. Contains the same `ChunkRecord` entries sorted by
key and stored in a B-tree for O(log N) floor queries.

```
<name>/
├── chunks.log      ← unchanged (append-only, kept as source of truth)
├── index.bxdb      ← magic header + B-tree of ChunkRecord entries
└── blobs/          ← unchanged
```

Blob files are never rewritten. Both formats reference them by
`(worker_id, offset, len)`.

### Format Detection

When opening a database, the library reads the first 8 bytes of whichever index
file is present:

- `index.bxdb` present → use B-tree mode
- `index.bxdb` absent, `chunks.log` present → use append-only scan mode
- Neither present → error

---

## 8. Conversion Tools

Two conversion directions are supported, both provided by the `bxdb-convert`
binary.

### Append-Only → B-Tree

```
bxdb-convert to-btree <name>
```

1. Open `chunks.log`, verify magic `BXDBLOG\0`.
2. Read all `ChunkRecord` entries.
3. Sort by combined key ascending.
4. Write `index.bxdb`: magic header `BXDBIDX\0` + version, then bulk-insert
   records in sorted order (maximises B-tree fill ratio).

After this step, `load_all_pages` uses the B-tree path automatically.

### B-Tree → Append-Only

```
bxdb-convert to-log <name>
```

1. Open `index.bxdb`, verify magic `BXDBIDX\0`.
2. Iterate all entries in key order.
3. Write a new `chunks.log`: magic header `BXDBLOG\0` + version, then emit
   records in iteration order.

Useful for exporting a database to a format that can be streamed or appended to,
or for moving a database to a machine where the B-tree index was not transferred.

Both conversions are idempotent. Blob files are never touched by either.

---

## 9. Read Path

### Single Page (`bxdb_load_page`)

```
1. Encode key = (pa << 19) | snapshot_id
2. B-tree floor query:
     index.range(..=key).next_back()
     where (result_key >> 19) == pa
   → no result: return false
3. Resolve chunk:
     Zero  → memset output to 0x00, return true

     Full  → check cache[chunk_key]
               hit:  copy cached page to output, return true
               miss: read blob, zstd decompress into output
                     insert output into cache[chunk_key]
                     return true

     Delta → check cache[base_key]
               hit:  base_page = cached page (no I/O, no decompress)
               miss: read base blob, zstd decompress base_page
                     insert base_page into cache[base_key]
             read delta blob
             apply XOR patch to base_page → output
             return true
```

Only `Full` chunks are inserted into the cache — they are the only ones that
require zstd decompression. `Delta` resolution (XOR patch) and `Zero` (memset)
are cheap enough that caching their output is unnecessary.

### All Pages (`bxdb_load_all_pages`)

`bxdb_load_all_pages` dispatches work differently depending on the format
detected at open time.

**B-tree mode** (after conversion):

The main thread enqueues `total_page_count` floor-query jobs
`(pa_offset + i, snapshot_id, output_slot = i)`. `worker_count` worker threads
each perform the single-page logic above, writing results directly into
`pages[i * 4096]`. The main thread waits for all workers before returning.

**Append-only scan mode** (before conversion):

Because `chunks.log` records pages in write order (chunk records for one
snapshot appear together, in ascending PA order within a snapshot), a single
linear scan is more efficient than `total_page_count` separate floor queries.

```
1. Allocate a table: latest[pa] → ChunkRecord, for pa in [pa_offset, pa_offset+total)
2. Scan chunks.log sequentially:
     for each ChunkRecord r:
       pa = r.key >> 19
       snap = r.key & 0x7FFFF
       if pa in range AND snap ≤ snapshot_id:
         if latest[pa] is empty OR snap > latest[pa].snap:
           latest[pa] = r
3. Distribute resolve jobs across worker_count workers:
     each worker resolves its assigned subset of latest[] entries,
     writing decompressed pages into pages[].
4. Zero-fill any pa with no entry found.
```

The sequential scan reads `chunks.log` exactly once, making it cache-friendly
for large bulk loads even without the B-tree index.

### Floor Query

B-tree keys are sorted `(PA ASC, snapshot_id ASC)`. The floor query:

```rust
tree.range(..=key)
    .next_back()
    .filter(|(k, _)| k >> 19 == pa)
```

Steps back from the upper bound to the highest stored snapshot ≤ the requested
one for that PA.

---

## 10. Shared-Memory Read Cache

Multiple reader processes share a single cache region backed by `mmap`. The
first process to open the database creates the region; subsequent processes
attach to it. Full details are in [cache.md](cache.md).

Summary:

| Parameter | Value |
|---|---|
| Page size | 4 KB |
| Ways per set | 16 |
| Total sets | 8192 |
| Total capacity | ~131K pages (~512 MB data) |
| Metadata overhead | 2 MB |
| Locking | One `pthread_mutex` (PROCESS_SHARED, ROBUST) per 16 sets |
| Eviction | Pseudo-LRU via 8-bit timestamp per slot |
| Hash function | Fibonacci hash of 64-bit combined key |

Cache keys are the 64-bit combined keys of **Full chunks only**. `Delta` and
`Zero` chunks are never inserted. This means:

- A `Full` chunk hit avoids one zstd decompress.
- A `Delta` chunk whose base is cached avoids one blob read and one zstd
  decompress; only the (small) delta blob is read and the XOR patch is applied.
- Multiple `Delta` requests sharing the same base Full chunk all hit the same
  single cache entry.

---

## 11. File Summary

| File | Written by | Read by | Notes |
|---|---|---|---|
| `chunks.log` | write workers, `bxdb-convert to-log` | `bxdb-convert to-btree`, readers | magic `BXDBLOG\0`; append-only |
| `blobs/worker_N.blob` | worker N | readers | append-only, never rewritten |
| `index.bxdb` | `bxdb-convert to-btree` | readers | magic `BXDBIDX\0`; B-tree, written once |
| shared memory region | first reader process | all reader processes | transient cache |

---

## 12. Design Decisions

| Decision | Rationale |
|---|---|
| Synchronous API | Simpler caller contract; write phase is already separate from read phase so blocking is acceptable |
| Bitmap scan + work queue dispatch | Workers process pages as they are discovered; no need to pre-count dirty pages or pre-partition ranges |
| Per-worker blob files | Eliminates file write contention; each worker appends independently |
| fsync before returning from save | Guarantees durability; crash after return leaves a consistent state |
| Adaptive Full chunk threshold | Creates a new Full base only when the delta exceeds `delta_threshold` words; avoids unnecessary Full writes while bounding read cost to 2 blob reads |
| Global shared shadow (`RwLock`) | All workers share one base-page table; prevents redundant Full chunks for the same PA across workers; write-lock contention is low because Full chunks are rare |
| zstd only for Full chunks | Delta patches are sparse `(u16, u64)` pairs; zstd on them adds decompression cost for negligible gain |
| Magic numbers in file headers | Format is self-identifying; `load_all_pages` detects append-only vs B-tree without external metadata |
| `load_all_pages` sequential scan on append-only | A single linear pass over `chunks.log` is more efficient than N floor queries for bulk loads |
| Two-way conversion (`to-btree` / `to-log`) | B-tree → append-only allows streaming/export and recovery if only blobs + index survive |
| Separate `bxdb-convert` binary | Write library stays simple; conversion can run offline or on a different machine |
| Shared-memory cache | Avoids redundant decompression when multiple reader processes run on the same host |
| 45+19 = 64-bit key packing | Single integer fits directly into B-tree, cache, and `HashMap` without extra indirection |
