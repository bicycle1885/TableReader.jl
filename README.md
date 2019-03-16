# TableReader.jl

[![Build Status](https://travis-ci.com/bicycle1885/TableReader.jl.svg?branch=master)](https://travis-ci.com/bicycle1885/TableReader.jl)
[![Codecov](https://codecov.io/gh/bicycle1885/TableReader.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/bicycle1885/TableReader.jl)

TableReader.jl does not waste your time.

Features:

- Carefully optimized for speed.
- Transparently decompresses gzip, xz, and zstd data.
- Read data from a local file, a remote file, or a running process.

Here is a quick benchmarking result:

    ~/w/TableReader (master|…) $ julia
                   _
       _       _ _(_)_     |  Documentation: https://docs.julialang.org
      (_)     | (_) (_)    |
       _ _   _| |_  __ _   |  Type "?" for help, "]?" for Pkg help.
      | | | | | | |/ _` |  |
      | | |_| | | | (_| |  |  Version 1.1.0 (2019-01-21)
     _/ |\__'_|_|_|\__'_|  |  Official https://julialang.org/ release
    |__/                   |

    julia> using TableReader, BenchmarkTools

    julia> @time readcsv("iris.csv");  # start-up time
      1.771506 seconds (2.74 M allocations: 136.838 MiB, 2.30% gc time)

    julia> @benchmark readcsv("iris.csv")  # parsing speed
    BenchmarkTools.Trial:
      memory estimate:  1.03 MiB
      allocs estimate:  94
      --------------
      minimum time:     82.711 μs (0.00% GC)
      median time:      88.433 μs (0.00% GC)
      mean time:        107.542 μs (12.68% GC)
      maximum time:     46.517 ms (99.56% GC)
      --------------
      samples:          10000
      evals/sample:     1

    ~/w/TableReader (master|…) $ julia -q
    julia> using CSV, DataFrames, BenchmarkTools

    julia> @time DataFrame(CSV.File("iris.csv"));
      7.892412 seconds (33.59 M allocations: 1.408 GiB, 8.70% gc time)

    julia> @benchmark DataFrame(CSV.File("iris.csv"))
    BenchmarkTools.Trial:
      memory estimate:  25.28 KiB
      allocs estimate:  356
      --------------
      minimum time:     209.578 μs (0.00% GC)
      median time:      213.480 μs (0.00% GC)
      mean time:        233.416 μs (3.66% GC)
      maximum time:     52.844 ms (93.27% GC)
      --------------
      samples:          10000
      evals/sample:     1

    ~/w/TableReader (master|…) $ julia -q
    julia> using TextParse, BenchmarkTools

    julia> @time csvread("iris.csv");
      4.743130 seconds (14.31 M allocations: 681.774 MiB, 6.98% gc time)

    julia> @benchmark csvread("iris.csv")
    BenchmarkTools.Trial:
      memory estimate:  127.16 KiB
      allocs estimate:  2331
      --------------
      minimum time:     192.463 μs (0.00% GC)
      median time:      199.889 μs (0.00% GC)
      mean time:        225.422 μs (7.25% GC)
      maximum time:     45.882 ms (99.21% GC)
      --------------
      samples:          10000
      evals/sample:     1


## Installation

This package depends on the latest version of
[TranscodingStreams.jl][transcodingstreams-url] (v0.9.2 or newer).  Please
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


## Design notes

TableReader.jl is aimed at users who want to keep the easy things easy.  Thus,
it exports a simple function, `readdlm`, that reads a tabular text file into a
dataframe.  For ease of use, `readcsv` and `readtsv` functions, thin wrapper
functions around `readdlm` with sensible default parameters, are also exported.
These two functions are for CSV and TSV file formats, respectively. No other
functions except the three are exported from this package.

The three functions takes an object as the source of tabular data. The source
object may be a filename, an URL string, a command, or any I/O object. For
example, the following examples will work as you expect:

```julia
readcsv("path/to/filename.csv")
readcsv("https://example.com/path/to/filename.csv")
readcsv(`unzip -p path/to/dataset.zip filename.csv`)
readcsv(IOBuffer(some_csv_data))
```

In addition, these functions guess the file format from the magic bytes if any.
Currently, plain text, gzip, xz, and zstd are detectable. These file formats
are transparently decompressed if required and thus the user does not need to
decompress a file in advance.

Column data types are guessed from the data. Currently, integers (`Int`),
floating-point numbers (`Float64`), dates (`Date`), datetimes (`DateTime`), and
strings (`String`) are supported. If empty cells (i.e., two consective
delimiters, or a delimiter and a newline) are found, they are interpreted as
missing values. Such a column is converted to a vector of
`Vector{Union{T,Missing}}`, where `T` refers to a data type guessed from
non-missing values.

To reduce memory usage, the parser of this package reads data chunk by chunk.
The default chunk size is 1 MiB, and data types are guessed using the bufferred
data in the first chunk. Although this strategy works in most cases, you may
encounter situation where most values in a column look like integers but only
few are not parsable as integers. If you are bad luck, such anomalies are not
in the first chunk and type guessing may fail. Consequently, parsing will also
fail when the parser sees the first occurrence.  To avoid the problem, you can
turn off the chunking behavior by setting the `chunksize` parameter to zero.
For example, `readcsv("somefile.csv", chunksize = 0)` will read the whole file
into memory as a single large chunk and column types are guessed from all of
the cells.  While this requires more memories, you will never see parsing error
due to the failure of type guessing.


## Limitations

The tokenizer cannot handle extremely long cells in a data file. The length of
a token is encoded using 24-bit integer, and therefore a cell that is longer
than or equal to 16 MiB will result in parsing failure. This is not likely to
happen, but please be careful if, for example, a column contains long strings.
Also, the size of a chunk is limited up to 64 GiB; you cannot disable chunking
if the data size is larger than that.

[transcodingstreams-url]: https://github.com/bicycle1885/TranscodingStreams.jl
