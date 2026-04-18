# Shared Memory Page Cache Design

## Overview

This document describes the design of a cross-process, shared-memory page cache
in Rust. The cache stores fixed-size 4KB pages and is accessible by multiple
processes simultaneously via a shared memory region backed by `mmap`.

The design is intentionally minimal: each cache entry stores only a page ID and
an LRU timestamp, keeping metadata overhead well under 1% of total cache size.

---

## Cache Structure

The cache uses a **set-associative** design, the same model used in CPU L2/L3
caches. This avoids the extremes of direct-mapped (high conflict miss rate) and
fully associative (requires global locking).

### Parameters

| Parameter        | Value              | Notes                              |
|------------------|--------------------|------------------------------------|
| Page size        | 4 KB               | Both data and metadata pages       |
| Ways per set     | 16                 | Linear scan, 3 cache lines         |
| Sets per metadata page | 16           | Share one lock per metadata page   |
| Metadata pages   | 512 (power of 2)   | For 100K entry cache               |
| Total sets       | 8192               | 512 pages × 16 sets                |
| Target entries   | ~100K              | 8192 sets × 16 ways = 131K slots   |

### Address Computation

A 64-bit hash of the page ID is used to locate the set:

```
hash     = hash(page_id)           // e.g. Fibonacci: wrapping_mul(0x9e3779b97f4a7c15)
meta_idx = (hash >> 4) & 511       // 9 bits → which of 512 metadata pages
set_idx  = hash & 0xF              // 4 bits → which of 16 sets within the page
way      = linear scan [0..16]     // scan for matching page_id or LRU eviction
data_idx = meta_idx * 256 + set_idx * 16 + way
```

Using Fibonacci hashing rather than `page_id % num_sets` avoids hot spots when
page IDs are sequential.

---

## Memory Layout

The shared region consists of two contiguous areas:

```
┌──────────────────────────────────────┐
│  Metadata region  (512 × 4KB = 2MB)  │  ← tags, locks, LRU state
├──────────────────────────────────────┤
│  Data region  (131K × 4KB ≈ 512MB)   │  ← raw page contents
└──────────────────────────────────────┘
```

All addressing uses **byte offsets from the base pointer**, never raw pointers,
because each process maps the region at a potentially different virtual address.

### Data Page Address

```
data_base + data_idx * 4096
```

---

## Metadata Page Layout

Each 4KB metadata page owns 16 sets × 16 ways = 256 cache slots, and carries
one `pthread_mutex` (padded to 64 bytes) that protects all 256 slots.

Within each set, `page_id` values and `timestamp` values are stored in separate
contiguous arrays rather than interleaved per entry. This improves cache locality
during lookup and eviction: a hit check only loads the `page_id` array (2 cache
lines), and an eviction scan only loads the `timestamp` array (1 cache line).
The data page itself is never touched until a hit or eviction target is confirmed.

```
┌─────────────────────────────────────┐  offset 0
│  pthread_mutex_t  (40B)             │
│  padding          (24B)             │  padded to 64B (one cache line)
├─────────────────────────────────────┤  offset 64
│  SetMetadata[16]  (3072B)           │  16 sets × 192B each
│                                     │
│  per set:                           │
│    page_ids   [u64; 16]  (128B)     │  ← 2 cache lines, hit check
│    timestamps [u8;  16]  ( 16B)     │  ← 1 cache line, eviction scan
│    padding               ( 48B)     │  pad SetMetadata to 192B
│                                     │
├─────────────────────────────────────┤  offset 3136
│  unused           (960B)            │  available for future use
└─────────────────────────────────────┘  offset 4096
```

In Rust:

```rust
#[repr(C)]
struct SetMetadata {
    page_ids:   [u64; 16],  // 128 bytes, 2 cache lines — u64::MAX = empty slot
    timestamps: [u8;  16],  //  16 bytes, 1 cache line  — 8-bit LRU clock
    _pad:       [u8;  48],  // pad to 192 bytes total
}

#[repr(C, align(4096))]
struct MetadataPage {
    mutex:  [u8; 64],           // pthread_mutex_t + padding
    sets:   [SetMetadata; 16],  // 16 × 192B = 3072 bytes
    _pad:   [u8; 960],          // spare space
}
// 64 + 3072 + 960 = 4096 ✓
```

### Lookup Cache Line Cost

| Operation         | Arrays accessed          | Cache lines |
|-------------------|--------------------------|-------------|
| Hit check         | `page_ids` only          | 2           |
| Eviction scan     | `timestamps` only        | 1           |
| Miss (no evict)   | `page_ids` only          | 2           |
| Full lookup+evict | `page_ids` + `timestamps`| 3           |

On a miss, the `timestamps` array is never loaded — the lookup terminates after
the `page_ids` scan alone.

---

## Locking Strategy

A single `pthread_mutex` initialized with `PTHREAD_PROCESS_SHARED` is embedded
at offset 0 of each metadata page. It protects all 16 sets (256 slots) within
that page.

Key properties:
- The mutex lives **inside** the shared region, so all processes access the same
  lock instance.
- `PTHREAD_MUTEX_ROBUST` is recommended so that if a process dies while holding
  the lock, other processes can detect and recover from the abandoned lock.
- Lock scope is small: a typical lookup acquires the lock, scans at most 2
  cache lines for a hit check, and at most 3 cache lines total when eviction
  is also needed, then releases.

---

## Memory Overhead

For a 100K entry cache:

```
Data region:      100K × 4KB  = 400 MB
Metadata region:  512  × 4KB  =   2 MB  (0.5% overhead)
Total:                           402 MB
```

The 512 metadata pages accommodate 131K slots (8192 sets × 16 ways), so roughly
31K slots are unused — these are cold 12-byte tag entries and cost nothing in
practice.

---

## Design Decisions & Rationale

| Decision | Rationale |
|---|---|
| 16-way associativity | Matches CPU L2/L3 cache design; 16-entry scan = 2–3 cache lines, fast under a held lock |
| Split page_id / timestamp arrays | Hit check loads only `page_ids` (2 cache lines); eviction scan loads only `timestamps` (1 cache line); data page untouched on miss |
| 8-bit LRU timestamp | Sufficient for clock-based eviction; 16 timestamps fit in a single cache line |
| One lock per metadata page | Balances parallelism vs. lock memory cost; 512 locks for 131K entries |
| Power-of-2 metadata page count | Enables branchless hash truncation with a single bit mask |
| Fibonacci hashing | Avoids clustering when page IDs are sequential or strided |
| Offset-based addressing | Required for shared memory: virtual addresses differ per process |
| `PTHREAD_MUTEX_ROBUST` | Allows surviving a process crash while holding the lock |

---

## Future Considerations

- The 960 spare bytes per metadata page could hold per-set clock hands, access
  counters, or a generation number for optimistic reads.
- The 48 padding bytes in `SetMetadata` are available for per-set state such as
  a clock hand position, dirty flags, or a pinned-page bitmap.
- A reader-count field (using atomics, no lock needed) would enable a
  reader-writer protocol where multiple readers pin a page without contention.
