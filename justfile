build-so:
    cargo build --release

build-inspect:
    cargo build --bin bxdb-inspect --features inspect --release

gen-header:
    cbindgen -l c --output c-test/bxdb.h

build-c-test TEST: build-so
    clang -o c-test/{{ TEST }} c-test/{{ TEST }}.c ./target/release/libbxdb.a -lpthread -ldl

build-zig-test TEST: build-so
    zig build-exe c-test/{{ TEST }}.zig -I c-test/ ./target/release/libbxdb.a -lc -lpthread -ldl -lunwind -femit-bin=c-test/{{ TEST }}

run-test TEST: build-inspect (build-zig-test TEST)
    ./c-test/{{ TEST }}
    ./target/release/bxdb-inspect {{ TEST }}

alias t := build-c-test
alias zt := build-zig-test
alias rt := run-test
