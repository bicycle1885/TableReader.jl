# TableReader.jl

[![Build Status](https://travis-ci.com/bicycle1885/TableReader.jl.svg?branch=master)](https://travis-ci.com/bicycle1885/TableReader.jl)
[![Codecov](https://codecov.io/gh/bicycle1885/TableReader.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/bicycle1885/TableReader.jl)

TableReader.jl does not waste your time.

- Carefully optimized for speed.
- Transparently decompresses gzip, xz, and zstd data.
- Read data from a local file, a remote file, or a running process.


## Installation

This package depends on the latest version of
[TranscodingStreams.jl](transcodingstreams-url) (v0.9.2 or newer).  Please
update it if it is older than required, and then add this package as follows:

    pkg> add https://github.com/bicycle1885/TableReader.jl


## Usage

```julia
# This takes the three functions into the current scope:
#   - readdlm
#   - readcsv
#   - readtsv
using TableReader

# Read a CSV file and return a DataFrame object.
dataframe = readcsv("somefile.csv")

# Read gzip/xz/zstd compressed files.
dataframe = readcsv("somefile.csv.gz")

# Read a remote file as downloading.
dataframe = readcsv("https://example.com/somefile.csv")

# Read stdout from a process.
dataframe = readcsv(`unzip -p data.zip somefile.csv`)
```

[transcodingstreams-url]: https://github.com/bicycle1885/TranscodingStreams.jl
