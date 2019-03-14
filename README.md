# TableReader.jl

[![Build Status](https://travis-ci.com/bicycle1885/TableReader.jl.svg?branch=master)](https://travis-ci.com/bicycle1885/TableReader.jl)
[![Codecov](https://codecov.io/gh/bicycle1885/TableReader.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/bicycle1885/TableReader.jl)

TableReader.jl does not waste your time.

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
