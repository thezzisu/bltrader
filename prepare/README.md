# BLTrader - IO

This module is responsible for the preprocess of data.

## Build

To use vendored sources, add this to your .cargo/config.toml for this project:

```
[source.crates-io]
replace-with = "vendored-sources"

[source.vendored-sources]
directory = "vendor"
```

If you're using anaconda, please set following environment variables:

```
export HDF5_DIR=/opt/anaconda3
export RUSTFLAGS="-C link-args=-Wl,-rpath,$HDF5_DIR/lib"
```

## Cache File Format

```
| Length (4 bytes) | Data (Length * sizeof(item) bytes) |
```
