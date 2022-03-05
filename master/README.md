# BLTrader - Master

This module is responsible for the IO part of BLTrader.
It offers a simple API to extract data from HFP5 files.

## RPC Protocol

```
RPC Request
Header
| Magic (4 bytes) | Method (1 bytes) |
Body (optional)
| Length (4 bytes) | Data (Length bytes) |

RPC Response
Header
| Magic (4 bytes) | Status (1 bytes) |
Body (optional)
| Length (4 bytes) | Data (Length bytes) |
```
