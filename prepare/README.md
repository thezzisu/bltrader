# BLTrader - IO

This module is responsible for the IO part of BLTrader.
It offers a simple API to extract data from HFP5 files.

## Cache File Format

```
| Length (4 bytes) | Data (Length * sizeof(item) bytes) |
```
