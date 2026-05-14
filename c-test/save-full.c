#include "bxdb.h"
#include <stdlib.h>
#include <stdint.h>

int main() {
    // Create a memory region with 64 pages.
    char *data = calloc(4096, 64);
    struct BxdbHandle *db = bxdb_open_for_append_only("test-save-full", 2, 0, true);

    bxdb_save_all_pages(db, data, 64, 0);

    bxdb_close(db);
    free(data);
    return 0;
}
