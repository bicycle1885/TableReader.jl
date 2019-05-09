# TableReader.jl

[![Docs Latest][docs-latest-img]][docs-latest-url]
[![Build Status](https://travis-ci.com/bicycle1885/TableReader.jl.svg?branch=master)](https://travis-ci.com/bicycle1885/TableReader.jl)
[![Codecov](https://codecov.io/gh/bicycle1885/TableReader.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/bicycle1885/TableReader.jl)

TableReader.jl does not waste your time.

Features:

- Carefully optimized for speed.
- Transparently decompresses gzip, xz, and zstd data.
- Read data from a local file, a remote file, or a running process.

Here is a quick benchmark of start-up time:

    ~/w/TableReader (master|…) $ julia
                   _
       _       _ _(_)_     |  Documentation: https://docs.julialang.org
      (_)     | (_) (_)    |
       _ _   _| |_  __ _   |  Type "?" for help, "]?" for Pkg help.
      | | | | | | |/ _` |  |
      | | |_| | | | (_| |  |  Version 1.1.0 (2019-01-21)
     _/ |\__'_|_|_|\__'_|  |  Official https://julialang.org/ release
    |__/                   |

    julia> using TableReader

    julia> @time readcsv("data/iris.csv");  # start-up time
      2.301008 seconds (2.80 M allocations: 139.657 MiB, 1.82% gc time)

    ~/w/TableReader (master|…) $ julia -q
    julia> using CSV, DataFrames

    julia> @time DataFrame(CSV.File("data/iris.csv"));  # start-up time
      7.443172 seconds (33.26 M allocations: 1.389 GiB, 9.05% gc time)

    ~/w/TableReader (master|…) $ julia -q
    julia> using CSVFiles, DataFrames

    julia> @time DataFrame(load("data/iris.csv"));  # start-up time
     12.578236 seconds (47.81 M allocations: 2.217 GiB, 9.87% gc time)

And the parsing throughput of TableReader.jl is often ~1.5-3.0 times faster
than those of pandas and other Julia packages. See [this
post](https://discourse.julialang.org/t/ann-tablereader-jl-a-fast-and-simple-csv-parser/22335)
for more selling points.


## Installation

Start a new session by the `julia` command, hit the <kbd>]</kbd> key to change
the mode, and run `add TableReader` in the `pkg>` prompt.


## Usage

```julia
# This takes the three functions into the current scope:
#   - readdlm
#   - readcsv
#   - readtsv
using TableReader

# Read a CSV file and return a DataFrame object.
dataframe = readcsv("somefile.csv")

# Automatic delimiter detection.
dataframe = readdlm("somefile.txt")

# Read gzip/xz/zstd compressed files.
dataframe = readcsv("somefile.csv.gz")

# Read a remote file as downloading.
dataframe = readcsv("https://example.com/somefile.csv")

# Read stdout from a process.
dataframe = readcsv(`unzip -p data.zip somefile.csv`)
```

The following parameters are available:

- `delim`: specify the delimiter character
- `quot`: specify the quotation character
- `trim`: trim space around fields
- `lzstring`: parse excess leading zeros as strings
- `skip`: skip the leading lines
- `skipblank`: skip blank lines
- `comment`: specify the leading sequence of comment lines
- `colnames`: set the column names
- `normalizenames`:  "normalize" column names into valid Julia (DataFrame) identifier symbols
- `hasheader`: notify the parser the existence of a header
- `chunkbits`: set the size of a chunk

See the docstring of `readdlm` for more details.


## Design

TableReader.jl is aimed at users who want to keep the easy things easy.  It
exports three functions: `readdlm`, `readcsv`, and `readtsv`. `readdlm` is at
the core of the package, and the other two functions are a thin wrapper that
calls `readdlm` with some default parameters; `readcsv` is for CSV files and
`readtsv` is for TSV files. These functions always return a data frame object
of DataFrames.jl. No other functions except the three are exported from this
package.

Things happen transparently:
1. The functions detect compression from data so users do not need to specify
   any parameters to notify the fact.
2. The data types of columns are guessed from data (integers, floats, bools,
   dates, datetimes, strings, and missings are supported).
3. If the data source looks like a URL, it is downloaded with the curl command.
4. `readdlm` detects the delimiter of fields from data (of course, you can
   force a specific delimiter using the `delim` parameter).

The three functions takes an object as the source of tabular data to read. It
may be a filename, a URL string, a command, or any kind of I/O objects.  For
example, the following examples will work as you expect:
```julia
readcsv("path/to/filename.csv")
readcsv("https://example.com/path/to/filename.csv")
readcsv(`unzip -p path/to/dataset.zip filename.csv`)
readcsv(IOBuffer(some_csv_data))
```

To reduce memory usage, the parser reads data chunk by chunk and the data types
are guessed using the buffered data in the first chunk. If the chunk size is
not enough to detect the types correctly, the parser will fail when it detects
unexpected data fields. You can expand the chunk size by the `chunkbits`
parameter; the default size is `chunkbits = 20`, which means 2^20 bytes (= 1
MiB).  If you set the value to zero (i.e., `chunkbits = 0`), the parser reads
the whole data file into a buffer without chunking it. This theoretically never
mistakes the data types in exchange for higher memory usage.


## Limitations

The tokenizer cannot handle extremely long fields in a data file. The length of
a token is encoded using 24-bit integer, and therefore a cell that is longer
than or equal to 16 MiB will result in parsing failure. This is not likely to
happen, but please be careful if, for example, there are columns that contain
long strings.  Also, the size of a chunk is limited up to 64 GiB; you cannot
disable chunking if the data size is larger than that.

[transcodingstreams-url]: https://github.com/bicycle1885/TranscodingStreams.jl
[docs-latest-img]: https://img.shields.io/badge/docs-latest-blue.svg
[docs-latest-url]: https://bicycle1885.github.io/TableReader.jl/latest
