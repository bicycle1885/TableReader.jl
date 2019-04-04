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


## Design notes

TableReader.jl is aimed at users who want to keep the easy things easy.  It
exports three functions: `readdlm`, `readcsv`, and `readtsv`. `readdlm` is at
the core of the package, and the other two functions are a thin wrapper that
calls `readdlm` with some default parameters; `readcsv` is for CSV files and
`readtsv` is for TSV files. These functions returns a data frame of
DataFrames.jl. No other functions except the three are exported from this
package.

The three functions takes an object as the source of tabular data to read. It
may be a filename, a URL string, a command, or any kind of I/O objects.  For
example, the following examples will work as you expect:

```julia
readcsv("path/to/filename.csv")
readcsv("https://example.com/path/to/filename.csv")
readcsv(`unzip -p path/to/dataset.zip filename.csv`)
readcsv(IOBuffer(some_csv_data))
```

In addition, the functions guess the file format from the magic bytes if any.
Currently, plain text, gzip, xz, and zstd are detectable. These file formats
are transparently decompressed if required and thus the user does not need to
decompress a file in advance.

Column data types are guessed from the data. Currently, integers (`Int`),
floating-point numbers (`Float64`), boolean values (`Bool`), dates (`Date`),
datetimes (`DateTime`), missing values (`Missing`), and strings (`String`) are
supported. If empty fields (i.e., two consective delimiters, or a delimiter and
a newline) or "NA" are found, they are interpreted as missing values. Such a
column is converted to a vector of `Vector{Union{T,Missing}}`, where `T` refers
to a data type guessed from non-missing values.

To reduce memory usage, the parser of this package reads data chunk by chunk.
The default chunk size is 1 MiB, and data types are guessed using the bufferred
data in the first chunk. Although this strategy works in most cases, you may
encounter situation where most values in a column look like integers but only
few are not parsable as integers. If you are bad luck, such anomalies are not
in the first chunk and type guessing may fail. Consequently, parsing will also
fail when the parser sees the first occurrence.  To avoid the problem, you can
turn off the chunking behavior by setting the `chunkbits` parameter to zero.
For example, `readcsv("somefile.csv", chunkbits = 0)` will read the whole file
into memory as a single large chunk and the data types of columns are guessed
from all of the fields.  While this requires more memories, you will never see
parsing error due to the failure of type guessing.


## Limitations

The tokenizer cannot handle extremely long fields in a data file. The length of
a token is encoded using 24-bit integer, and therefore a cell that is longer
than or equal to 16 MiB will result in parsing failure. This is not likely to
happen, but please be careful if, for example, a column contains long strings.
Also, the size of a chunk is limited up to 64 GiB; you cannot disable chunking
if the data size is larger than that.

[transcodingstreams-url]: https://github.com/bicycle1885/TranscodingStreams.jl
[docs-latest-img]: https://img.shields.io/badge/docs-latest-blue.svg
[docs-latest-url]: https://bicycle1885.github.io/TableReader.jl/latest
