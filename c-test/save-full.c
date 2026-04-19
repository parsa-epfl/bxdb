#include "bxdb.h"
#include <stdlib.h>
#include <stdint.h>

int main() {
    // Create a memory region with 64 pages.
    char *data = calloc(4096, 64);
    uint64_t dirty_bitmap = 0xffffffffffffffff;
    struct BxdbHandle *db = bxdb_open_for_fw("test-save-full", 2, 0);

    bxdb_save_pages(db, data, &dirty_bitmap, 64, 0);

    bxdb_close(db);
    free(data);
    return 0;
}
