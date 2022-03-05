# BLTrader - IO

This module is responsible for the preprocess of data.

## Build

If you're using anaconda, please set following environment variables:

```
export HDF5_DIR=/opt/anaconda3
export RUSTFLAGS="-C link-args=-Wl,-rpath,$HDF5_DIR/lib"
```

## Cache File Format

```
| Length (4 bytes) | Data (Length * sizeof(item) bytes) |
```
