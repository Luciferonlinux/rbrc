# Rust 1brc

This is my implementation of [gunnarmorling/1brc](github.com/gunnarmorling/1brc)
in Rust.

## Benchmarks

Times are measured using the `criterion` crate and the file is served from a
ramdisk. Multithreaded versions will be run with 8 worker threads (`-j 8`)\
System spec: Ryzen 7 7800x3D and 32GB DDR5 Memory at 6000MT/s.

| Classic | 10k Keys | I/O  | Hashmap                     | Notes                   |
| :-----: | :------: | :--- | :-------------------------- | :---------------------- |
| 3.34 s  |  3.80 s  | mmap | `std::collections::HashMap` | Baseline Implementation |
