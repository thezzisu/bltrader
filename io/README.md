# BLTrader - IO

This module is responsible for the IO part of BLTrader.
It offers a simple API to extract data from HFP5 files.

## API Protocol

```
Request
| Method ID(4 byte) | Payload Length(4 bytes) | Payload |
Response
| Payload Length(4 bytes) | Payload |
```
