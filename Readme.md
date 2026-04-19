# Rust 1brc

This is my implementation of [gunnarmorling/1brc](github.com/gunnarmorling/1brc)
in Rust.

## Benchmark results

Times are measured using the `time` command line utility and the file is served
from a ramdisk. Multithreaded versions will be run with 8 worker threads
(`-j 8`)\
System spec: Ryzen 7 7800x3D and 32GB DDR5 Memory at 6000MT/s.

| Time  | I/O  | Hashmap                     | Parsing                        | Notes                   |
| :---: | :--- | :-------------------------- | :----------------------------- | :---------------------- |
| 4.1 s | mmap | `std::collections::HashMap` | unaligned SWAR Integer parsing | Baseline Implementation |
