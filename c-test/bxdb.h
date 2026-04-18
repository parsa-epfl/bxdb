#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define CACHE_PAGE_SIZE 4096

#define METADATA_PAGES 512

#define SETS_PER_META 16

#define WAYS_PER_SET 16

#define TOTAL_SLOTS ((METADATA_PAGES * SETS_PER_META) * WAYS_PER_SET)

#define METADATA_BYTES (METADATA_PAGES * CACHE_PAGE_SIZE)

#define DATA_BYTES (TOTAL_SLOTS * CACHE_PAGE_SIZE)

#define TOTAL_BYTES (METADATA_BYTES + DATA_BYTES)

#define PAGE_SIZE 4096

#define PAGE_WORDS (PAGE_SIZE / 8)

#define SNAPSHOT_BITS 19

#define SNAPSHOT_MASK ((1ull << SNAPSHOT_BITS) - 1)

#define MAX_SNAPSHOT_ID ((1u << SNAPSHOT_BITS) - 1)

#define DEFAULT_DELTA_THRESHOLD 256

#define FORMAT_VERSION 1

#define HEADER_SIZE 16

#define CHUNK_FULL 0

#define CHUNK_DELTA 1

#define CHUNK_ZERO 2

#define FIXED_RECORD_SIZE 32

typedef struct BxdbHandle BxdbHandle;

struct BxdbHandle *bxdb_init(void);

struct BxdbHandle *bxdb_open_for_write(const char *name,
                                       int worker_count,
                                       uint16_t delta_threshold);

struct BxdbHandle *bxdb_open_for_read(const char *name);

void bxdb_close(struct BxdbHandle *db);

void bxdb_save_pages(struct BxdbHandle *db,
                     const char *memory,
                     const uint64_t *dirty_bitmap,
                     uint64_t total_page_count,
                     uint32_t snapshot_id);

bool bxdb_load_page(struct BxdbHandle *db, char *page, uint64_t pa, uint32_t snapshot_id);

bool bxdb_load_all_pages(struct BxdbHandle *db,
                         char *pages,
                         uint64_t pa_offset,
                         uint64_t total_page_count,
                         uint32_t snapshot_id,
                         int worker_count);
