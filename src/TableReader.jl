module TableReader

export readdlm, readcsv, readtsv

using Dates:
    Date,
    DateTime,
    @dateformat_str
using Unicode:
    isletter
using DataFrames:
    DataFrame
using TranscodingStreams:
    TranscodingStreams,
    TranscodingStream,
    NoopStream,
    Memory,
    Buffer,
    State,
    Noop,
    fillbuffer,
    makemargin!,
    buffermem
using CodecZlib:
    GzipDecompressorStream
using CodecZstd:
    ZstdDecompressorStream
using CodecXz:
    XzDecompressorStream

const SP = UInt8(' ')
const CR = UInt8('\r')
const LF = UInt8('\n')

include("stringcache.jl")
include("tokenizer.jl")
include("parser.jl")

const DEFAULT_CHUNK_SIZE =  1 * 2^20  #  1 MiB
const MINIMUM_CHUNK_SIZE = 16 * 2^10  # 16 KiB

# Printable characters
const CHARS_PRINT = ' ':'~'

# Whitelist of delimiters and quotation marks
const ALLOWED_DELIMITERS = tuple(['\t'; CHARS_PRINT[.!(isletter.(CHARS_PRINT) .| isdigit.(CHARS_PRINT))]]...)
const ALLOWED_QUOTECHARS = tuple(CHARS_PRINT[.!(isletter.(CHARS_PRINT) .| isdigit.(CHARS_PRINT))]...)

"""
    readdlm(filename, command, or IO object;
            delim,
            quot = '"',
            trim = true,
            lzstring = true,
            skip = 0,
            skipblank = true,
            colnames = nothing,
            hasheader = (colnames === nothing),
            chunksize = 1 MiB)

Read a character delimited text file.

[`readcsv`](@ref) and [`readtsv`](@ref) call this function behind. To read a
CSV or TSV file, consider to use these dedicated function instead.


## Data source

The first (and the only positional) argument specifies the source to read data
from there.

If the argument is a string, it is considered as a local file name or the URL
of a remote file. If the name matches with `r"^\\w+://.*"` in regular
expression, it is handled as a URL. For example,
`"https://example.com/path/to/file.csv"` is regarded as a URL and its content
is streamed using the `curl` command.

If the argument is a command object, it is considered as a source whose
standard output is text data to read. For example, `unzip -p path/to/file.zip
somefile.csv` can be used to extract a file from a zipped archive. It is also
possible to pipeline several commands using `pipeline`.

If the arguments is an object of the `IO` type, it is considered as a direct
data source. The content is read using `read` or other similar functions. For
example, passing `IOBuffer(text)` makes it possible to read data from the raw
text object.

The data source is transparently decompressed if the compression format is
detectable. Currently, gzip, zstd, and xz are supported. The format is detected
by the magic bytes of the stream header, and therefore other information such
as file names does not affect the detection.


## Parser parameters

`delim` specifies the field delimiter in a line.  This cannot be the same
character as `quot`.  Currently, the following delimiters are allowed:
$(join(repr.(ALLOWED_DELIMITERS), ", ")).

`quot` specifies the quotation character to enclose a field. This cannot be the
same character as `delim`. Currently, the following quotation characters are
allowed: $(join(repr.(ALLOWED_QUOTECHARS), ", ")).

`trim` specifies whether the parser trims space (0x20) characters around a field.
If `trim` is true, `delim` and `quot` cannot be a space character.

`lzstring` specifies whether fields with excess leading zeros are treated as
strings.  If `lzstring` is true, fields such as "0003" will be interpreted as
strings instead of integers.

`skip` specifies the number of lines to skip before reading data.  The next
line just after the skipped lines is considered as a header line if the
`colnames` parameter is not specified.

`skipblank` specifies whether the parser ignores blank lines. If `skipblank` is
false, encountering a blank line throws an exception.


## Column names

`colnames` specifies the column names. If `colnames` is `nothing` (default),
the column names are read from the first line just after skipping lines
specified by `skip` (no lines are skipped by default). Any iterable object is
allowed.

`hasheader` specified whether the data has a header line or not. The default
value is `colnames === nothing` and thus the parser assumes there is a header
if and only if no column names are specified.

The following table summarizes the behavior of the `colnames` and `hasheader`
parameters.

| `colnames` | `hasheader` | column names |
|:-----------|:------------|:-------------|
| `nothing` | `true`  | taken from the header (default) |
| `nothing` | `false` | automatically generated (X1, X2, ...) |
| specified | `true`  | taken from `colnames` (the header line is skipped) |
| specified | `false` | taken from `colnames` |

If unnamed columns are found in the header, they are renamed to `UNNAMED_{j}`
for ease of access, where `{j}` is replaced by the column number. If the number
of header columns in a file is less than the number of data columns by one, a
column name `UNNAMED_0` will be inserted into the column names as the first
column.  This is useful to read files written by the `write.table` function of
R with `row.names = TRUE`.


## Data types

Integers, floating-point numbers, boolean values, dates, datetimes, missings,
and strings are automatically detected and converted from the text data.  The
following list is the summary of the corresponding data types of Julia and the
text formats described in the regular expression:

- Integer (`Int`): `[-+]?\\d+`
- Float (`Float64`): `[-+]?\\d*\\.?\\d+`, `[-+]?\\d*\\.?\\d+([eE][-+]?\\d+)?`,
                     `[-+]?NaN` or `[-+]?Inf(inity)?` (case-insensitive)
- Bool (`Bool`): `t(rue)?` or `f(alse)?` (case-insensitive)
- Date (`Dates.Date`): `\\d{4}-\\d{2}-\\d{2}`
- Datetime (`Dates.DateTime`): `\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?`
- Missing (`Missing`): empty field or `NA` (case-sensitive)
- String (`String`): otherwise

Integers and floats have some overlap. The parser precedes integers over
floats.  That means, if all values in a column are parsable as integers and
floats, they are parsed as integers instead of floats; otherwise, they are
parsed as floats. Similarly, all the types have higher precedence than strings.

The parser parameter `lzstring` affects interpretation of numbers. If
`lzstring` is true, numbers with excess leading zeros (e.g., "0001", "00.1")
are interpreted as strings. Fields without excess leading zeros (e.g., "0",
"0.1") are interepreted as numbers regardless of this parameter.


## Parsing behavior

The only supported text encoding of a file is UTF-8, which is the default
character encoding scheme of many functions in Julia.  If you need to read text
encoded other than UTF-8, it is required to wrap the data stream with an
encoding conversion tool such as the `iconv` command or StringEncodings.jl.

```julia
# Convert text encoding from Shift JIS (Japanese) to UTF8.
readcsv(`iconv -f sjis -t utf8 somefile.csv`)
```

A text file will be read chunk by chunk to save memory. The chunk size is
specified by the `chunksize` parameter, which is set to 1 MiB by default.  The
data type of each column is guessed from the values in the first chunk.  If
`chunksize` is set to zero, it disables chunking and the data types are guessed
from all rows. The chunk size will be automatically expanded when it is
required to store long lines.

A chunk cannot be larger than 64 GiB and a field cannot be longer than 16 MiB.
These limits are due to the encoding method of tokens used by the tokenizer.
Therefore, you cannot parse data larger than 64 GiB without chunking and fields
longer than 16 MiB. Trying to read such a file will result in error.
"""
function readdlm end

"""
    readcsv(filename or IO object; delim = ',', <keyword arguments>)

Read a CSV (comma-separated values) text file.

This function is the same as [`readdlm`](@ref) but with `delim = ','`.
See `readdlm` for details.
"""
function readcsv end

"""
    readtsv(filename or IO object; delim = '\\t', <keyword arguments>)

Read a TSV (tab-separated values) text file.

This function is the same as [`readdlm`](@ref) but with `delim = '\\t'`.
See `readdlm` for details.
"""
function readtsv end

for (fname, delim) in [(:readdlm, nothing), (:readcsv, ','), (:readtsv, '\t')]
    # prepare keyword arguments
    kwargs = Expr[]
    if delim === nothing
        push!(kwargs, :(delim::Char))
    else
        push!(kwargs, Expr(:kw, :(delim::Char), delim))
    end
    push!(kwargs, Expr(:kw, :(quot::Char), '"'))  # quot::Char = '"'
    push!(kwargs, Expr(:kw, :(trim::Bool), true))  # trim::Bool = true
    push!(kwargs, Expr(:kw, :(lzstring::Bool), true))  # lzstring::Bool = true
    push!(kwargs, Expr(:kw, :(skip::Integer), 0))  # skip::Integer = 0
    push!(kwargs, Expr(:kw, :(skipblank::Bool), true))  # skipblank::Bool = true
    push!(kwargs, Expr(:kw, :(colnames), nothing))  # colnames = nothing
    push!(kwargs, Expr(:kw, :(hasheader::Bool), :(colnames === nothing)))  # hasheader::Bool = (colnames === nothing)
    push!(kwargs, Expr(:kw, :(chunksize::Integer), DEFAULT_CHUNK_SIZE))  # chunksize::Integer = DEFAULT_CHUNK_SIZE

    # generate methods
    @eval begin
        function $(fname)(filename::AbstractString; $(kwargs...))
            params = ParserParameters(delim, quot, trim, lzstring, skip, skipblank, colnames, hasheader, chunksize)
            if occursin(r"^\w+://", filename)  # URL-like filename
                if Sys.which("curl") === nothing
                    throw(ArgumentError("the curl command is not available"))
                end
                # read a remote file using curl
                return open(proc -> readdlm_internal(wrapstream(proc, params), params), `curl --silent $(filename)`)
            end
            # read a local file
            return open(file -> readdlm_internal(wrapstream(file, params), params), filename)
        end

        function $(fname)(cmd::Base.AbstractCmd; $(kwargs...))
            params = ParserParameters(delim, quot, trim, lzstring, skip, skipblank, colnames, hasheader, chunksize)
            return open(proc -> readdlm_internal(wrapstream(proc, params), params), cmd)
        end

        function $(fname)(file::IO; $(kwargs...))
            params = ParserParameters(delim, quot, trim, lzstring, skip, skipblank, colnames, hasheader, chunksize)
            return readdlm_internal(wrapstream(file, params), params)
        end
    end
end

# Wrap a stream by TranscodingStream.
function wrapstream(stream::IO, params::ParserParameters)
    bufsize = max(params.chunksize, MINIMUM_CHUNK_SIZE)
    if !applicable(mark, stream) || !applicable(reset, stream)
        stream = NoopStream(stream, bufsize = bufsize)
    end
    format = checkformat(stream)
    if params.chunksize != 0
        # with chunking
        if format == :gzip
            return GzipDecompressorStream(stream, bufsize = bufsize)
        elseif format == :zstd
            return ZstdDecompressorStream(stream, bufsize = bufsize)
        elseif format == :xz
            return XzDecompressorStream(stream, bufsize = bufsize)
        elseif stream isa TranscodingStream
            return stream
        else
            return NoopStream(stream, bufsize = bufsize)
        end
    end
    # without chunking
    if format == :gzip
        data = read(GzipDecompressorStream(stream))
    elseif format == :zstd
        data = read(ZstdDecompressorStream(stream))
    elseif format == :xz
        data = read(XzDecompressorStream(stream))
    else
        data = read(stream)
    end
    buffer = Buffer(data)
    return TranscodingStream(Noop(), devnull, State(buffer, buffer))
end

# Check the file format of a stream.
function checkformat(stream::IO)
    mark(stream)
    magic = zeros(UInt8, 6)
    nb = readbytes!(stream, magic, 6)
    reset(stream)
    if nb != 6
        return :unknown
    elseif magic[1:6] == b"\xFD\x37\x7A\x58\x5A\x00"
        return :xz
    elseif magic[1:2] == b"\x1f\x8b"
        return :gzip
    elseif magic[1:4] == b"\x28\xb5\x2f\xfd"
        return :zstd
    else
        return :unknown
    end
end

# The main function of parsing a character delimited file.
# `stream` is asuumed to be an input stream of plain text.
function readdlm_internal(stream::TranscodingStream, params::ParserParameters)
    # Determine column names
    line = skiplines(stream, params.skip) + 1
    if params.skipblank
        line += skipblanlines(stream, params.trim)
    end
    mem, lastnl = bufferlines(stream)
    if params.hasheader
        if params.colnames === nothing
            # Scan the header line to get the column names
            n, headertokens = scanheader(mem, lastnl, params)
            skip(stream, n)
            if length(headertokens) == 1 && length(headertokens[1]) == 0  # zero-length token
                throw(ReadError("found no column names in the header at line $(line)"))
            end
            line += 1
            colnames = Symbol[]
            for (i, token) in enumerate(headertokens)
                start, length = location(token)
                if (kind(token) & QSTRING) != 0
                    name = qstring(mem, start, length, params.quot)
                else
                    name = unsafe_string(mem.ptr + start - 1, length)
                end
                if isempty(name)  # unnamed column
                    name = "UNNAMED_$(i)"
                end
                push!(colnames, Symbol(name))
            end
        else
            # skip the header line
            line += skiplines(stream, 1)
            colnames = params.colnames
        end
    else
        # no header line
        if params.colnames === nothing
            # count the number of columns from data
            n_max_cols = countbytesline(mem, params.delim) + 1
            _, n_cols = scanline!(
                Array{Token}(undef, (n_max_cols, 1)), 1, mem, 0, lastnl, line, params)
            colnames = [Symbol("X", i) for i in 1:n_cols]
        else
            colnames = params.colnames
        end
    end
    ncols = length(colnames)

    # Scan the next line to get the number of data columns
    if params.skipblank
        line += skipblanlines(stream, params.trim)
    end
    mem, lastnl = bufferlines(stream)
    tokens = Array{Token}(undef, (ncols + 1, 1))
    _, i = scanline!(tokens, 1, mem, 0, lastnl, line, params)
    if i == 1 && location(tokens[1,1])[2] == 0
        # no data
        return DataFrame([[] for _ in 1:length(colnames)], colnames, makeunique = true)
    elseif i == ncols
        # the header and the first row have the same number of columns
    elseif i == ncols + 1
        # the first column is supposed to be unnamed
        ncols += 1
        pushfirst!(colnames, :UNNAMED_0)
    else
        throw(ReadError("unexpected number of columns at line $(line)"))
    end

    # Estimate the number of rows.
    nrows_estimated = countbytes(mem, LF)
    if nrows_estimated == 0
        nrows_estimated = countbytes(mem, CR)
    end
    n_chunk_rows = max(nrows_estimated, 5)  # allocate at least five rows

    # Read data.
    tokens = Array{Token}(undef, (ncols, n_chunk_rows))
    columns = Vector[]
    while !eof(stream)
        # Tokenize data.
        mem, lastnl = bufferlines(stream)
        pos = 0
        n_new_rows = 0
        while pos < lastnl && n_new_rows < n_chunk_rows
            pos, i = scanline!(tokens, n_new_rows + 1, mem, pos, lastnl, line, params)
            if pos == 0
                break
            elseif params.skipblank && i == 1 && length(tokens[1,n_new_rows+1]) == 0
                line += 1
                continue
            elseif i != ncols
                throw(ReadError("unexpected number of columns at line $(line)"))
            else
            end
            n_new_rows += 1
            line += 1
        end
        if n_new_rows == 0
            # only blank lines
            continue
        end

        # Parse data.
        bitmaps = summarizecolumns(tokens, n_new_rows)
        if isempty(columns)
            # infer data types of columns
            resize!(columns, ncols)
            for i in 1:ncols
                T = datatype(bitmaps[i])
                @debug "Filling $(colnames[i])::$(T) column"
                columns[i] = fillcolumn!(
                    Vector{T}(undef, n_new_rows),
                    n_new_rows, mem, tokens, i, params.quot)
            end
        else
            # check existing columns
            for i in 1:ncols
                col = columns[i]
                S = eltype(col)
                T = datatype(bitmaps[i])
                U = Union{S,T}
                if (S <: Union{Int,Missing} && T <: Union{Float64,Missing}) ||
                   (T <: Union{Int,Missing} && S <: Union{Float64,Missing})
                    U = promote_type(S, T)
                elseif !(U <: S || U <: T)
                    throw(ReadError(string(
                        "type guessing failed at column $(i) ",
                        "(guessed to be $(S) but found records of $(T)); ",
                        "try larger chunksize or chunksize = 0 to disable chunking")))
                end
                n_rows = length(col)
                if T <: S
                    resize!(col, n_rows + n_new_rows)
                else
                    col = copyto!(Vector{U}(undef, n_rows + n_new_rows), 1, col, 1, n_rows)
                end
                @debug "Filling $(colnames[i])::$(U) column"
                columns[i] = fillcolumn!(col, n_new_rows, mem, tokens, i, params.quot)
            end
        end
        skip(stream, pos)
        if params.chunksize == 0
            # Without chunking, reading data must finish in a pass.
            @assert eof(stream)
        end
    end

    # Parse strings as date or datetime objects.
    for i in 1:ncols
        col = columns[i]
        if eltype(col) <: Union{String,Missing}
            if is_date_like(col)
                try
                    columns[i] = parse_date(col)
                catch
                    # not a date column
                end
            elseif is_datetime_like(col)
                hasT = occursin('T', col[1])  # check delimited by T or space
                try
                    columns[i] = parse_datetime(col, hasT)
                catch
                    # not a datetime column
                end
            end
        end
    end

    return DataFrame(columns, colnames, makeunique = true)
end

# Count the number of `byte` in a memory block.
function countbytes(mem::Memory, byte::UInt8)
    n = 0
    @inbounds @simd for i in 1:length(mem)
        n += mem[i] == byte
    end
    return n
end

# Count the number of `byte` in a line.
function countbytesline(mem::Memory, byte::UInt8)
    n = 0
    for i in 1:lastindex(mem)
        @inbounds x = mem[i]
        if x == CR || x == LF
            break
        elseif x == byte
            n += 1
        end
    end
    return n
end

function datatype(bitmap::UInt8)
    if (bitmap & 0b010000) != 0  # all values are missing
        return Missing
    end
    T = (bitmap & INTEGER) != 0 ? Int     :
        (bitmap & FLOAT)   != 0 ? Float64 :
        (bitmap & BOOL)    != 0 ? Bool    : String
    if (bitmap & 0b100000) != 0  # at least one value is missing
        T = Union{T,Missing}
    end
    return T
end

# Summarize columns using bitmaps.
function summarizecolumns(tokens::Matrix{Token}, nrows::Int)
    # From the least significant bit:
    #   1. INTEGER
    #   2. FLOAT
    #   3. BOOL
    #   4. QSTRING
    #   5. ∀ missing
    #   6. ∃ missing
    ncols = size(tokens, 1)
    bitmaps = Vector{UInt8}(undef, ncols)
    fill!(bitmaps, 0b011111)
    @inbounds for j in 1:nrows, i in 1:ncols
        # Note that the tokens matrix is transposed.
        x = kind(tokens[i,j])
        ismissing = x == 0b1111
        y = bitmaps[i]
        bitmap = 0b000000
        bitmap |= (y & 0b100000) | ifelse(ismissing, 0b100000, 0b000000)
        bitmap |= (y & 0b010000) & ifelse(ismissing, 0b010000, 0b000000)
        bitmap |= (y & 0b001111) & x
        bitmaps[i] = bitmap
    end
    return bitmaps
end

# Skip the number of lines specified by `skip`.
function skiplines(stream::TranscodingStream, skip::Int)
    skipped = 0
    while skipped < skip && !eof(stream)
        mem, lastnl = bufferlines(stream)
        # find the first newline
        i = 1
        while i ≤ lastnl
            @inbounds x = mem[i]
            if x == CR
                if i + 1 ≤ lastindex(mem) && mem[i+1] == LF
                    i += 1
                end
                break
            elseif x == LF
                break
            end
            i += 1
        end
        Base.skip(stream, i)
        skipped += 1
    end
    return skipped
end

# Skip blank lines.
function skipblanlines(stream::TranscodingStream, trim::Bool)
    skipped = 0
    while !eof(stream)
        mem, lastnl = bufferlines(stream)
        @assert lastnl > 0
        i = 1
        if trim
            while i ≤ lastindex(mem) && mem[i] == SP
                i += 1
            end
        end
        if i + 1 ≤ lastindex(mem) && mem[i] == CR && mem[i+1] == LF
            skip(stream, i + 1)
        elseif i ≤ lastindex(mem) && (mem[i] == CR || mem[i] == LF)
            skip(stream, i)
        else
            # found a non-blank line
            break
        end
        skipped += 1
    end
    return skipped
end

# Buffer lines and return the memory view and the last newline position.
function bufferlines(stream::TranscodingStream)
    # try to find the last newline (LF, CR, or CR+LF)
    @label SEARCH_NEWLINE
    fillbuffer(stream, eager = true)
    buffer = stream.state.buffer1
    mem = buffermem(buffer)
    lastnl = lastindex(mem)
    while lastnl > 0
        @inbounds x = mem[lastnl]
        if x == LF
            break
        elseif x == CR
            if lastnl == lastindex(mem)  # cannot determine CR or CR+LF
                makemargin!(buffer, 1)
                fillbuffer(stream, eager = true)
                mem = buffermem(buffer)
                lastnl = lastindex(mem)
                while lastnl > 0
                    @inbounds x = mem[lastnl]
                    if x == LF || x == CR
                        break
                    end
                    lastnl -= 1
                end
            end
            break
        end
        lastnl -= 1
    end
    if lastnl == 0  # found no newlines in the buffered chunk
        if TranscodingStreams.marginsize(buffer) == 0
            # No newlines in the current buffer but newlines may be in the next.
            bufsize = length(buffer)
            if bufsize ≥ MAX_TOKEN_START
                throw(ReadError("found a too long line to store in a chunk"))
            end
            newbufsize = min(bufsize * 2, MAX_TOKEN_START)
            TranscodingStreams.makemargin!(buffer, newbufsize - bufsize)
            @goto SEARCH_NEWLINE
        else
            # Found EOF without newlines; terminate the buffered data with LF.
            TranscodingStreams.writebyte!(buffer, LF)
            mem = buffermem(buffer)
            lastnl = lastindex(mem)
        end
    end
    @assert lastnl > 0 && (mem[lastnl] == CR || mem[lastnl] == LF)
    return mem, lastnl
end

# Generated from tools/snoop.jl.
function _precompile_()
    ccall(:jl_generating_output, Cint, ()) == 1 || return nothing
    precompile(Tuple{typeof(TableReader.scanline!), Array{TableReader.Token, 2}, Int64, TranscodingStreams.Memory, Int64, Int64, Int64, TableReader.ParserParameters})
    precompile(Tuple{typeof(TableReader.scanheader), TranscodingStreams.Memory, Int64, TableReader.ParserParameters})
    precompile(Tuple{typeof(TableReader.checkformat), Base.Process})
    precompile(Tuple{typeof(TableReader.checkformat), TranscodingStreams.TranscodingStream{TranscodingStreams.Noop, Base.Process}})
    precompile(Tuple{typeof(TableReader.bufferlines), TranscodingStreams.TranscodingStream{TranscodingStreams.Noop, Base.IOStream}})
    precompile(Tuple{typeof(TableReader.checkformat), TranscodingStreams.TranscodingStream{TranscodingStreams.Noop, Base.IOStream}})
    precompile(Tuple{typeof(TableReader.checkformat), Base.IOStream})
    precompile(Tuple{typeof(TableReader.parse_date), Array{Union{Base.Missing, String}, 1}})
    precompile(Tuple{typeof(TableReader.fillcolumn!), Array{Int64, 1}, Int64, TranscodingStreams.Memory, Array{TableReader.Token, 2}, Int64, UInt8})
    precompile(Tuple{typeof(TableReader.countbytes), TranscodingStreams.Memory, UInt8})
    precompile(Tuple{typeof(TableReader.skipblanlines), TranscodingStreams.TranscodingStream{TranscodingStreams.Noop, Base.IOStream}, Bool})
    precompile(Tuple{typeof(TableReader.readdlm_internal), TranscodingStreams.TranscodingStream{TranscodingStreams.Noop, Base.IOStream}, TableReader.ParserParameters})
    precompile(Tuple{typeof(TableReader.qstring), TranscodingStreams.Memory, Int64, Int64, UInt8})
    precompile(Tuple{typeof(TableReader.summarizecolumns), Array{TableReader.Token, 2}, Int64})
    precompile(Tuple{typeof(TableReader.parse_date), Array{String, 1}})
    precompile(Tuple{typeof(TableReader.parse_datetime), Array{Union{Base.Missing, String}, 1}})
    precompile(Tuple{typeof(TableReader.fillcolumn!), Array{Float64, 1}, Int64, TranscodingStreams.Memory, Array{TableReader.Token, 2}, Int64, UInt8})
    precompile(Tuple{typeof(TableReader.parse_datetime), Array{String, 1}})
    precompile(Tuple{typeof(TableReader.allocate!), TableReader.StringCache, Ptr{UInt8}, Int64})
    precompile(Tuple{typeof(TableReader.skiplines), TranscodingStreams.TranscodingStream{TranscodingStreams.Noop, Base.IOStream}, Int64})
    precompile(Tuple{typeof(TableReader.wrapstream), Base.Process, TableReader.ParserParameters})
    precompile(Tuple{typeof(TableReader.wrapstream), Base.IOStream, TableReader.ParserParameters})
    precompile(Tuple{typeof(TableReader.fillcolumn!), Array{String, 1}, Int64, TranscodingStreams.Memory, Array{TableReader.Token, 2}, Int64, UInt8})
    precompile(Tuple{typeof(TableReader.is_date_like), Array{String, 1}})
    precompile(Tuple{typeof(TableReader.is_datetime_like), Array{String, 1}})
    precompile(Tuple{typeof(TableReader.readcsv), String})
    precompile(Tuple{typeof(TableReader.readtsv), String})
    precompile(Tuple{getfield(TableReader, Symbol("##readcsv#12")), Char, Char, Bool, Int64, Bool, Nothing, Int64, typeof(identity), String})
    precompile(Tuple{getfield(TableReader, Symbol("##readtsv#21")), Char, Char, Bool, Int64, Bool, Nothing, Int64, typeof(identity), String})
end
_precompile_()

end # module
