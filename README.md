# Stream Join

Check the project [report](./report.pdf) for details.

## Overview

This repo contains 2 stream join implementations:

1. **Broadcast join**: single master - multiple worker dispatching diagram
2. **Handshake join**: multiple worker pipeline flow join diagram

see `include/join/` for details

The local window join on each worker can be configured to use different local indexes:

- List (Nested Loop Join)
- [B+ Tree](https://github.com/bingmann/stx-btree)
- [Alex Map](https://github.com/microsoft/ALEX), which has memory leak bugs if run it for a long time.
- [PGM Index](https://github.com/gvinciguerra/PGM-index)

## Code Structure

- Move your data stream for testing under `data/` directory (`data/SOSD/books_200M_uint32`, `data/tpc-h/customer.tbl`, ...)
- `main.py` takes the parameters in `config.yml` to run the stream join with different configurations.
- `benchmark.py` is used to evaluate the performance of the different join implementations & local indexes (build with Release mode before running).
- `include/` contains the implementation details of the different join algorithms and local indexes.
  - `include/join/` contains the implementation of the join algorithms (handshake/broadcast).
  - `include/index/` contains the implementation of the local (wrapper) indexes.
  - `include/stream/` contains the implementation of the stream interfaces and naive simulations.
