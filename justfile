zig := "/home/sqlin/.zvm/bin/zig"

build-so:
    cargo build -p bxdb-core --release

build-inspect:
    cargo build -p bxdb-inspect --release

gen-header:
    cbindgen -l c --output c-test/bxdb.h

build-c-test TEST: build-so
    clang -o c-test/{{ TEST }} c-test/{{ TEST }}.c ./target/release/libbxdb.a -lpthread -ldl

build-zig-test TEST: build-so
    {{zig}} build-exe c-test/{{ TEST }}.zig -I c-test/ ./target/release/libbxdb.a -lc -lpthread -ldl -lunwind -femit-bin=c-test/{{ TEST }}

run-test TEST: build-inspect (build-zig-test TEST)
    ./c-test/{{ TEST }}
    ./target/release/bxdb-inspect {{ TEST }}

build-stress-save: build-so
    {{zig}} build-exe c-test/stress-parallel-save.zig -I c-test/ ./target/release/libbxdb.a -lc -lpthread -ldl -lunwind -femit-bin=c-test/stress-parallel-save

build-timing-serve: build-so
    {{zig}} build-exe c-test/timing-serve.zig -I c-test/ ./target/release/libbxdb.a -lc -lpthread -ldl -lunwind -femit-bin=c-test/timing-serve

build-stress-snapdiff: build-so
    {{zig}} build-exe c-test/stress-snapshot-diff.zig -I c-test/ ./target/release/libbxdb.a -lc -lpthread -ldl -lunwind -femit-bin=c-test/stress-snapshot-diff

run-stress-save: build-stress-save
    ./c-test/stress-parallel-save

run-timing-serve: build-timing-serve
    ./target/release/bxdb-convert to-btree stress-save-64w
    bash c-test/run-timing-serve.sh

run-stress-snapdiff: build-stress-snapdiff
    ./c-test/stress-snapshot-diff

alias t := build-c-test
alias zt := build-zig-test
alias rt := run-test
