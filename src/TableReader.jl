module TableReader

export readdlm, readtsv, readcsv

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
    buffermem
using CodecZlib:
    GzipDecompressorStream
using CodecZstd:
    ZstdDecompressorStream
using CodecXz:
    XzDecompressorStream

const DEFAULT_CHUNK_SIZE =  1 * 2^20  #  1 MiB
const MINIMUM_CHUNK_SIZE = 16 * 2^10  # 16 KiB

"""
    readdlm(filename or IO object;
            delim,
            quot = '"',
            trim = true,
            header = nothing,
            chunksize = $(DEFAULT_CHUNK_SIZE))

Read a character delimited text file.

`delim` specifies the field delimiter in a line. This must be tab, space, or an
ASCII punctuation character.

`quot` specifies the quotation to enclose a field. This cannot be the same
character as `delim`.

`trim` specifies whether the parser trims space (0x20) characters around a field.
If `trim` is true, `delim` and `quot` cannot be a space character.

`header` specifies the column names. If `header` is `nothing` (default), the
column names are read from the first line of the text file. Any iterable object
is allowed.

If unnamed columns are found in the header, they are renamed to `UNNAMED_{j}`
for ease of access, where `{j}` is replaced by the column number.

A text file will be read chunk by chunk to save memory. The chunk size is
specified by the `chunksize` parameter, which is set to 1 MiB by default.
The data type of each column is guessed from the values in the first chunk.
"""
function readdlm end

"""
    readtsv(filename or IO object; delim = '\\t', <keyword arguments>)

Read a TSV text file.

This function is the same as [`readdlm`](@ref) but with `delim = '\\t'`.
See `readdlm` for details.
"""
function readtsv end

"""
    readcsv(filename or IO object; delim = ',', <keyword arguments>)

Read a CSV text file.

This function is the same as [`readdlm`](@ref) but with `delim = ','`.
"""
function readcsv end

for (fname, delim) in [(:readdlm, nothing), (:readtsv, '\t'), (:readcsv, ',')]
    # prepare keyword arguments
    kwargs = Expr[]
    if delim === nothing
        push!(kwargs, :(delim::Char))
    else
        push!(kwargs, Expr(:kw, :(delim::Char), delim))
    end
    push!(kwargs, Expr(:kw, :(quot::Char), '"'))  # quot::Char = '"'
    push!(kwargs, Expr(:kw, :(trim::Bool), true))  # trim::Bool = true
    push!(kwargs, Expr(:kw, :(header), nothing))  # header = nothing
    push!(kwargs, Expr(:kw, :(chunksize::Integer), DEFAULT_CHUNK_SIZE))  # chunksize::Integer = DEFAULT_CHUNK_SIZE

    # generate methods
    @eval begin
        function $(fname)(filename::AbstractString; $(kwargs...))
            params = check_parser_parameters(delim, quot, trim, header, chunksize)
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
            params = check_parser_parameters(delim, quot, trim, header, chunksize)
            return open(proc -> readdlm_internal(wrapstream(proc, params), params), cmd)
        end

        function $(fname)(file::IO; $(kwargs...))
            params = check_parser_parameters(delim, quot, trim, header, chunksize)
            return readdlm_internal(wrapstream(file, params), params)
        end
    end
end

const SP = UInt8(' ')
const CR = UInt8('\r')
const LF = UInt8('\n')

# Printable characters
const CHARS_PRINT = ' ':'~'

# Whitelist of delimiters
const ALLOWED_DELIMITERS = tuple(['\t'; ' '; CHARS_PRINT[ispunct.(CHARS_PRINT)]]...)

# A set of parser parameters.
struct ParserParameters
    delim::UInt8
    quot::UInt8
    trim::Bool
    colnames::Union{Vector{Symbol},Nothing}
    chunksize::Int
end

function check_parser_parameters(delim::Char, quot::Char, trim::Bool, header::Any, chunksize::Integer)
    if delim ∉ ALLOWED_DELIMITERS
        throw(ArgumentError("delimiter $(repr(delim)) is not allowed"))
    elseif quot ∉ ALLOWED_DELIMITERS
        throw(ArgumentError("quotation mark $(repr(quot)) is not allowed"))
    elseif delim == quot
        throw(ArgumentError("delimiter and quotation mark cannot be the same character"))
    elseif delim == ' ' && trim
        throw(ArgumentError("delimiting with space and space trimming are exclusive"))
    elseif quot == ' ' && trim
        throw(ArgumentError("quoting with space and space trimming are exclusive"))
    elseif chunksize < 0
        throw(ArgumentError("chunks size cannot be negative"))
    end
    if header != nothing
        colnames = Symbol.(collect(header))
    else
        colnames = nothing
    end
    return ParserParameters(
        UInt8(delim),
        UInt8(quot),
        trim,
        colnames,
        chunksize,
    )
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

function readdlm_internal(stream::TranscodingStream, params::ParserParameters)
    delim, quot, trim = params.delim, params.quot, params.trim
    chunking = params.chunksize != 0
    if params.colnames === nothing
        colnames = readheader(stream, delim, quot, trim)
    else
        colnames = params.colnames
    end
    if any(name -> name === Symbol(""), colnames)
        rename_unnamed_columns!(colnames)
    end
    ncols = length(colnames)
    if ncols == 0
        return DataFrame()
    end
    fillbuffer(stream, eager = true)
    buffer = stream.state.buffer1
    nrows_estimated = countlines(buffermem(buffer))
    if nrows_estimated == 0
        nrows_estimated = countlines(buffermem(buffer), byte = CR)
    end
    nrows_estimated += 1  # maybe insert a newline at the end
    n_chunk_rows = nrows_estimated
    tokens = Array{Token}(undef, (ncols, n_chunk_rows))
    #fill!(tokens, Token(0x00, 0, 0))
    columns = Vector[]
    line = 2
    while !eof(stream)
        mem = buffermem(buffer)
        lastnl = find_last_newline(mem)
        if lastnl == 0 && fillbuffer(stream, eager = true) == 0
            # reached EOF without newline marker, so insert an LF to cheat the parser
            TranscodingStreams.makemargin!(buffer, 1)
            TranscodingStreams.writebyte!(buffer, LF)
            mem = buffermem(buffer)
            lastnl = find_last_newline(mem)
        end
        # maybe, found a line that is too long?
        @assert lastnl > 0
        pos = 0
        chunk_begin = line
        if chunking
            while pos < lastnl && line - chunk_begin + 1 ≤ n_chunk_rows
                pos = scanline!(tokens, line - chunk_begin + 1, mem, pos, lastnl, line, delim, quot, trim)
                line += 1
            end
        else
            while pos < lastnl
                @assert line - chunk_begin + 1 ≤ n_chunk_rows
                pos = scanline!(tokens, line - chunk_begin + 1, mem, pos, lastnl, line, delim, quot, trim)
                line += 1
            end
        end
        n_new_records = line - chunk_begin
        bitmaps = aggregate_columns(tokens, n_new_records)
        if isempty(columns)
            # infer data types of columns
            resize!(columns, ncols)
            for i in 1:ncols
                parsable = bitmaps[i] & 0b0111
                hasmissing = (bitmaps[i] & 0b1000) != 0
                T = (parsable & INTEGER) != 0 ? Int :
                    (parsable & FLOAT) != 0 ? Float64 : String
                if hasmissing
                    T = Union{T,Missing}
                end
                col = Vector{T}(undef, n_new_records)
                @debug "Filling $(colnames[i])::$(T) column"
                fillcolumn!(col, n_new_records, mem, tokens, i, quot)
                columns[i] = col
            end
        else
            # check existing columns
            for i in 1:ncols
                parsable = bitmaps[i] & 0b0111
                hasmissing = (bitmaps[i] & 0b1000) != 0
                col = columns[i]
                T = eltype(col)
                if T <: Union{Int,Missing}
                    (parsable & INTEGER) == 0 && throw(ReadError("type guessing failed at column $(i)"))
                elseif T <: Union{Float64,Missing}
                    (parsable & FLOAT) == 0 && throw(ReadError("type guessing failed at column $(i)"))
                else
                    @assert T <: Union{String,Missing}
                end
                if hasmissing && !(T >: Union{T,Missing})
                    # copy data to a new column
                    col = copyto!(Vector{Union{T,Missing}}(undef, length(col) + n_new_records), 1, col, 1, length(col))
                else
                    # resize the column for new records
                    resize!(col, length(col) + n_new_records)
                end
                @debug "Filling $(colnames[i])::$(T) column"
                fillcolumn!(col, n_new_records, mem, tokens, i, quot)
                columns[i] = col
            end
        end
        skip(stream, pos)
        if !chunking
            @assert eof(stream)
        end
        fillbuffer(stream, eager = true)
    end
    return DataFrame(columns, colnames)
end

# Count the number of lines in a memory block.
function countlines(mem::Memory; byte::UInt8 = LF)
    n = 0
    @inbounds @simd for i in 1:length(mem)
        n += mem[i] == byte
    end
    return n
end

# token kind
const STRING  = 0b0000
const INTEGER = 0b0001
const FLOAT   = 0b0010
const QSTRING = 0b0100  # string with quotation marks
const MISSING = 0b1011  # missing can be any data type

const MAX_TOKEN_START = 2^36
const MAX_TOKEN_LENGTH = 2^24

struct Token
    # From most significant
    #    4bit: kind (+ missing)
    #   36bit: start position (64 GiB)
    #   24bit: length (16 MiB)
    value::UInt64

    function Token(kind::UInt8, start::Int, len::Int)
        @assert start < MAX_TOKEN_START
        @assert len < MAX_TOKEN_LENGTH
        return new((UInt64(kind) << 60) | (UInt64(start) << 24) | UInt64(len))
    end
end

const TOKEN_NULL = Token(0x00, 0, 0)

function kind(token::Token)
    return (token.value >> 60) % UInt8
end

function ismissing(token::Token)
    return (token.value & (UInt64(1) << 63)) != 0
end

function location(token::Token)
    x = token.value & (~UInt64(0) >> 4)
    return (x >> 24) % Int, (x & (~UInt64(0) >> 40)) % Int
end

function aggregate_columns(tokens::Matrix{Token}, nrows::Int)
    ncols = size(tokens, 1)
    bitmaps = Vector{UInt8}(undef, ncols)
    fill!(bitmaps, 0b0111)
    @inbounds for j in 1:nrows, i in 1:ncols
        # Note that the tokens matrix is transposed.
        x = kind(tokens[i,j])
        y = bitmaps[i]
        bitmaps[i] = ((x | y) & 0b1000) | ((x & y) & 0b0111)
    end
    return bitmaps
end

function fillcolumn!(col::Vector{Int}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int, quot::UInt8)
    @inbounds for i in 1:nvals
        start, length = location(tokens[c,i])
        col[end-nvals+i] = parse_integer(mem, start, length)
    end
    return col
end

function fillcolumn!(col::Vector{Union{Int,Missing}}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int, quot::UInt8)
    @inbounds for i in 1:nvals
        t = tokens[c,i]
        if ismissing(t)
            col[end-nvals+i] = missing
        else
            start, length = location(t)
            col[end-nvals+i] = parse_integer(mem, start, length)
        end
    end
    return col
end

const SAFE_INT_LENGTH = sizeof(string(typemax(Int))) - 1

@inline function parse_integer(mem::Memory, start::Int, length::Int)
    stop = start + length - 1
    if length > SAFE_INT_LENGTH
        # safe but slow fallback
        buf = IOBuffer()
        for i in start:stop
            write(buf, mem[i])
        end
        return parse(Int, String(take!(buf)))
    end
    i = start
    b = mem[i]
    if b == UInt8('-')
        sign = -1
        i += 1
    elseif b == UInt8('+')
        sign = +1
        i += 1
    else
        sign = +1
    end
    n::Int = 0
    while i ≤ stop
        @inbounds b = mem[i]
        n = 10n + (b - UInt8('0'))
        i += 1
    end
    return sign * n
end

function fillcolumn!(col::Vector{Float64}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int, quot::UInt8)
    @inbounds for i in 1:nvals
        start, length = location(tokens[c,i])
        col[end-nvals+i] = parse_float(mem, start, length)
    end
    return col
end

function fillcolumn!(col::Vector{Union{Float64,Missing}}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int, quot::UInt8)
    @inbounds for i in 1:nvals
        t = tokens[c,i]
        if ismissing(t)
            col[end-nvals+i] = missing
        else
            start, length = location(tokens[c,i])
            col[end-nvals+i] = parse_float(mem, start, length)
        end
    end
    return col
end

@inline function parse_float(mem::Memory, start::Int, length::Int)
    return ccall(:strtod, Cdouble, (Ptr{UInt8}, Ptr{Cvoid}), mem.ptr + start - 1, C_NULL)
    #=  The above would be safe and faster.
    hasvalue, val = ccall(:jl_try_substrtod, Tuple{Bool,Float64}, (Ptr{UInt8}, Csize_t, Csize_t), mem.ptr, start-1, length)
    @assert hasvalue
    return val
    =#
end

include("stringcache.jl")
const N_CACHED_STRINGS = 8

function fillcolumn!(col::Vector{String}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int, quot::UInt8)
    cache = StringCache(N_CACHED_STRINGS)
    usecache = true
    @inbounds for i in 1:nvals
        t = tokens[c,i]
        start, length = location(t)
        if kind(t) & QSTRING != 0
            s = qstring(mem, start, length, quot)
        elseif usecache
            s = allocate!(cache, mem.ptr + start - 1, length % UInt64)
        else
            s = unsafe_string(mem.ptr + start - 1, length)
        end
        col[end-nvals+i] = s
        if usecache && i % 4096 == 0 && 10 * cache.stats.hit < cache.stats.hit + cache.stats.miss
            # stop using cache because the cache hit rate is too low
            usecache = false
        end
    end
    if !usecache
        @debug "Cache is turned off"
    end
    @debug cache
    return col
end

function fillcolumn!(col::Vector{Union{String,Missing}}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int, quot::UInt8)
    cache = StringCache(N_CACHED_STRINGS)
    usecache = true
    @inbounds for i in 1:nvals
        t = tokens[c,i]
        if ismissing(t)
            col[end-nvals+i] = missing
        else
            start, length = location(t)
            if kind(t) & QSTRING != 0
                s = qstring(mem, start, length, quot)
            elseif usecache
                s = allocate!(cache, mem.ptr + start - 1, length % UInt64)
            else
                s = unsafe_string(mem.ptr + start - 1, length)
            end
            col[end-nvals+i] = s
            if usecache && i % 4096 == 0 && 10 * cache.stats.hit < cache.stats.hit + cache.stats.miss
                # stop using cache because the cache hit rate is too low
                usecache = false
            end
        end
    end
    if !usecache
        @debug "Cache is turned off"
    end
    @debug cache
    return col
end

function qstring(mem::Memory, start::Int, length::Int, quot::UInt8)
    i = start
    skip = false
    buf = IOBuffer()
    while i < start + length
        if skip
            skip = false
        else
            x = mem[i]
            write(buf, x)
            skip = x == quot
        end
        i += 1
    end
    return String(take!(buf))
end

# Read header and return column names.
function readheader(stream::TranscodingStream, delim::UInt8, quot::UInt8, trim::Bool)
    fillbuffer(stream, eager = true)
    mem = buffermem(stream.state.buffer1)
    nl = find_first_newline(mem, 1)
    if nl == 0
        # TODO: maybe, the header is too long to be stored in the memory buffer
        return Symbol[]
    end
    n, tokens = scanheader(mem, 0, nl, delim, quot, trim)
    skip(stream, n)
    colnames = Symbol[]
    for token in tokens
        start, length = location(token)
        if (kind(token) & QSTRING) != 0
            name = qstring(mem, start, length, quot)
        else
            name = unsafe_string(mem.ptr + start - 1, length)
        end
        push!(colnames, Symbol(name))
    end
    return colnames
end

function rename_unnamed_columns!(colnames::Vector{Symbol})
    for i in 1:length(colnames)
        name = colnames[i]
        if name == Symbol("")
            colnames[i] = Symbol("UNNAMED_$(i)")
        end
    end
    return colnames
end

function find_first_newline(mem::Memory, i::Int)
    last = lastindex(mem)
    @inbounds while i ≤ last
        x = mem[i]
        if x == CR
            if i + 1 ≤ last && mem[i+1] == LF
                i += 1
            end
            break
        elseif x == LF
            break
        end
        i += 1
    end
    if i > last
        return 0
    end
    return i
end

function find_last_newline(mem::Memory)
    i = lastindex(mem)
    while i > 0
        @inbounds x = mem[i]
        if x == LF || x == CR
            break
        end
        i -= 1
    end
    return i
end


# Line parser
# -----------

struct ReadError <: Exception
    msg::String
end

macro state(name, ex)
    @assert name isa Symbol
    @assert ex isa Expr && ex.head == :block
    quote
        @label $(name)
        #println($(QuoteNode(name)))
        #@show quoted
        pos += 1
        #if pos > pos_end
        #    @goto END
        #end
        @inbounds c = mem[pos]
        #@show Char(c)
        #println()
        $(ex)
        # This area is unreachable because transition must be exhaustive.
        @assert false
    end |> esc
end

macro multibytestring()
    quote
        # multibyte UTF8 character
        if 0b110_00000 ≤ c ≤ 0b110_11111
            if pos + 1 ≤ pos_end && (mem[pos+1] >> 6) ≤ 0b10  # same as: 0b10_000000 ≤ mem[pos+1] ≤ 0b10_111111
                pos += 1
                @goto STRING
            end
        elseif 0b1110_0000 ≤ c ≤ 0b1110_1111
            if pos + 2 ≤ pos_end && (max(mem[pos+1], mem[pos+2]) >> 6) ≤ 0b10
                pos += 2
                @goto STRING
            end
        elseif 0b11110_000 ≤ c ≤ 0b11110_111
            if pos + 3 ≤ pos_end && (max(mem[pos+1], mem[pos+2], mem[pos+3]) >> 6) ≤ 0b10
                pos += 3
                @goto STRING
            end
        else
            @goto ERROR
        end
    end |> esc
end

macro follows(s)
    @assert s isa String && isascii(s)
    i = 0
    foldl(s, init = :(pos + $(sizeof(s)) ≤ pos_end)) do ex, c
        up = UInt8(uppercase(c))
        lo = UInt8(lowercase(c))
        i += 1
        :($(ex) && (mem[pos+$(i)] == $(up) || mem[pos+$(i)] == $(lo)))
    end |> esc
end

macro begintoken()
    esc(:(start = pos))
end

macro recordtoken(kind)
    quote
        token = Token($(kind), start, pos - start)
    end |> esc
end

macro endheadertoken()
    quote
        push!(tokens, token)
        quoted = false
        qstring = false
    end |> esc
end

function scanheader(mem::Memory, pos::Int, nl::Int, delim::UInt8, quot::UInt8, trim::Bool)
    tokens = Token[]
    token = TOKEN_NULL
    quoted = false
    qstring = false
    start = 0
    pos_end = nl

    @state BEGIN begin
        @begintoken
        if c == quot
            if quoted
                @recordtoken STRING
                @goto QUOTE_END
            end
            quoted = true
            @goto BEGIN
        elseif c == delim
            if quoted
                @goto STRING
            end
            @recordtoken STRING
            @endheadertoken
            @goto BEGIN
        elseif c == SP
            if trim && !quoted
                @goto BEGIN
            end
            @goto STRING
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == LF
            @recordtoken STRING
            @endheadertoken
            @goto END
        elseif c == CR
            @recordtoken STRING
            @endheadertoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state STRING begin
        if quoted && c == quot
            if qstring
                @recordtoken QSTRING
            else
                @recordtoken STRING
            end
            @goto QUOTE_END
        elseif c == delim
            if quoted
                @goto STRING
            end
            if qstring
                @recordtoken QSTRING
            else
                @recordtoken STRING
            end
            @endheadertoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == SP
            if trim && !quoted
                if qstring
                    @recordtoken QSTRING
                else
                    @recordtoken STRING
                end
                @goto STRING_SPACE
            end
            @goto STRING
        elseif c == LF
            if qstring
                @recordtoken QSTRING
            else
                @recordtoken STRING
            end
            @recordtoken STRING
            @endheadertoken
            @goto END
        elseif c == CR
            @recordtoken STRING
            @endheadertoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state STRING_SPACE begin
        if c == SP
            @goto STRING_SPACE
        elseif c == delim
            @endheadertoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == LF
            @endheadertoken
            @goto END
        elseif c == CR
            @endheadertoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state QUOTE_END begin
        if c == delim
            @endheadertoken
            @goto BEGIN
        elseif c == quot
            qstring = true
            @goto STRING
        elseif c == SP
            if trim
                @goto QUOTE_END_SPACE
            end
            @goto ERROR
        elseif c == LF
            @endheadertoken
            @goto END
        elseif c == CR
            @endheadertoken
            @goto CR
        else
            @goto ERROR
        end
    end

    @state QUOTE_END_SPACE begin
        if c == delim
            @endheadertoken
            @goto BEGIN
        elseif c == SP
            @goto QUOTE_END_SPACE
        elseif c == LF
            @endheadertoken
            @goto END
        elseif c == CR
            @endheadertoken
            @goto CR
        else
            @goto ERROR
        end
    end

    @label ERROR
    throw(ReadError("invalid file header format"))

    @label CR
    if pos + 1 ≤ pos_end && mem[pos + 1] == LF
        pos += 1
    end

    @label END
    return pos, tokens
end

macro endtoken()
    quote
        if i > ncols
            @goto ERROR
        end
        @inbounds tokens[i,row] = token
        i += 1
        quoted = false
        qstring = false
    end |> esc
end

# Scan a line in mem; mem must include one or more lines.
function scanline!(
        # output info
        tokens::Matrix{Token}, row::Int,
        # input info
        mem::Memory, pos::Int, lastnl::Int, line::Int,
        # parser parameters
        delim::UInt8, quot::UInt8, trim::Bool
    )

    # Check parameters.
    @assert delim != quot
    @assert !trim || delim != SP
    @assert !trim || quot != SP

    # Initialize variables.
    pos_end = lastnl
    ncols = size(tokens, 1)
    quoted = false
    qstring = false
    token = TOKEN_NULL
    start = 0  # the starting position of a token
    i = 1  # the current token

    @state BEGIN begin
        @begintoken
        if c == quot
            if quoted
                @recordtoken MISSING
                @goto QUOTE_END
            else
                quoted = true
                @goto BEGIN
            end
        elseif c == delim
            if quoted
                @goto STRING
            end
            @recordtoken MISSING
            @endtoken
            @goto BEGIN
        elseif c == UInt8('-') || c == UInt8('+')
            @goto SIGN
        elseif UInt8('0') ≤ c ≤ UInt8('9')
            @goto INTEGER
        elseif c == SP
            if trim && !quoted
                @goto BEGIN
            end
            @goto STRING
        elseif c == UInt8('.')
            @goto DOT
        elseif (c == UInt8('N') || c == UInt8('n')) && @follows("AN")  # case-insensitive
            # NaN
            pos += 2  # for 'A' and 'N'
            @goto SPECIAL_FLOAT
        elseif c == UInt8('I') || c == UInt8('i')
            # Infinity
            if @follows("NFINITY")
                pos += 7
                @goto SPECIAL_FLOAT
            elseif @follows("NF")
                pos += 2
                @goto SPECIAL_FLOAT
            end
            @goto STRING
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == LF
            if i == ncols
                @recordtoken MISSING
            end
            @endtoken
            @goto END
        elseif c == CR
            if i == ncols
                @recordtoken MISSING
            end
            @endtoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state SIGN begin
        if quoted && c == quot
            @recordtoken STRING
            @goto QUOTE_END
        elseif c == delim
            if quoted
                @goto STRING
            end
            @recordtoken STRING
            @endtoken
            @goto BEGIN
        elseif UInt8('0') ≤ c ≤ UInt8('9')
            @goto INTEGER
        elseif c == UInt8('.')
            @goto DOT
        elseif c == SP
            if trim && !quoted
                @recordtoken STRING
                @goto STRING_SPACE
            end
            @goto STRING
        elseif (c == UInt8('N') || c == UInt8('n')) && @follows("AN")
            pos += 2
            @goto SPECIAL_FLOAT
        elseif c == UInt8('I') || c == UInt8('i')
            # Infinity
            if @follows("NFINITY")
                pos += 7
                @goto SPECIAL_FLOAT
            elseif @follows("NF")
                pos += 2
                @goto SPECIAL_FLOAT
            end
            @goto STRING
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == LF
            @recordtoken STRING
            @endtoken
            @goto END
        elseif c == CR
            @recordtoken STRING
            @endtoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state INTEGER begin
        if quoted && c == quot
            @recordtoken INTEGER|FLOAT
            @goto QUOTE_END
        elseif c == delim
            if quoted
                @goto STRING
            end
            @recordtoken INTEGER|FLOAT
            @endtoken
            @goto BEGIN
        elseif UInt8('0') ≤ c ≤ UInt8('9')
            @goto INTEGER
        elseif c == UInt8('.')
            @goto POINT_FLOAT
        elseif c == SP
            if trim && !quoted
                @recordtoken INTEGER|FLOAT
                @goto INTEGER_SPACE
            end
            @goto STRING
        elseif c == UInt8('e') || c == UInt8('E')
            @goto EXPONENT
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == LF
            @recordtoken INTEGER|FLOAT
            @endtoken
            @goto END
        elseif c == CR
            @recordtoken INTEGER|FLOAT
            @endtoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state INTEGER_SPACE begin
        if c == SP
            @goto INTEGER_SPACE
        elseif c == delim
            @endtoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == LF
            @endtoken
            @goto END
        elseif c == CR
            @endtoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state DOT begin
        if quoted && c == quot
            @recordtoken STRING
            @goto QUOTE_END
        elseif c == delim
            if quoted
                @goto STRING
            end
            @recordtoken STRING
            @endtoken
            @goto BEGIN
        elseif UInt8('0') ≤ c ≤ UInt8('9')
            @goto POINT_FLOAT
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == SP
            if trim && !quoted
                @recordtoken STRING
                @goto STRING_SPACE
            end
            @goto STRING
        elseif c == LF
            @recordtoken STRING
            @endtoken
            @goto END
        elseif c == CR
            @recordtoken STRING
            @endtoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state POINT_FLOAT begin
        if quoted && c == quot
            @recordtoken FLOAT
            @goto QUOTE_END
        elseif c == delim
            if quoted
                @goto STRING
            end
            @recordtoken FLOAT
            @endtoken
            @goto BEGIN
        elseif UInt8('0') ≤ c ≤ UInt8('9')
            @goto POINT_FLOAT
        elseif c == UInt8('e') || c == UInt8('E')
            @goto EXPONENT
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == SP
            if trim && !quoted
                @recordtoken FLOAT
                @goto FLOAT_SPACE
            end
            @goto STRING
        elseif c == LF
            @recordtoken FLOAT
            @endtoken
            @goto END
        elseif c == CR
            @recordtoken FLOAT
            @endtoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state EXPONENT begin
        if quoted && c == quot
            @recordtoken STRING
            @goto QUOTE_END
        elseif c == delim
            if quoted
                @goto STRING
            end
            @recordtoken STRING
            @endtoken
            @goto BEGIN
        elseif UInt8('0') ≤ c ≤ UInt8('9')
            @goto EXPONENT_FLOAT
        elseif c == UInt8('-') || c == UInt8('+')
            @goto EXPONENT_SIGN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == SP
            if trim && !quoted
                @recordtoken STRING
                @goto STRING_SPACE
            end
            @goto STRING
        elseif c == LF
            @recordtoken STRING
            @endtoken
            @goto END
        elseif c == CR
            @recordtoken STRING
            @endtoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state EXPONENT_SIGN begin
        if quoted && c == quot
            @recordtoken STRING
            @goto QUOTE_END
        elseif c == delim
            if quoted
                @goto STRING
            end
            @recordtoken STRING
            @endtoken
            @goto BEGIN
        elseif UInt8('0') ≤ c ≤ UInt8('9')
            @goto EXPONENT_FLOAT
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == SP
            if trim && !quoted
                @recordtoken STRING
                @goto STRING_SPACE
            end
            @goto STRING
        elseif c == LF
            @recordtoken STRING
            @endtoken
            @goto END
        elseif c == CR
            @recordtoken STRING
            @endtoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state EXPONENT_FLOAT begin
        if quoted && c == quot
            @recordtoken FLOAT
            @goto QUOTE_END
        elseif c == delim
            if quoted
                @goto STRING
            end
            @recordtoken FLOAT
            @endtoken
            @goto BEGIN
        elseif UInt8('0') ≤ c ≤ UInt8('9')
            @goto EXPONENT_FLOAT
        elseif c == SP
            if trim && !quoted
                @recordtoken FLOAT
                @goto FLOAT_SPACE
            end
            @goto STRING
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == LF
            @recordtoken FLOAT
            @endtoken
            @goto END
        elseif c == CR
            @recordtoken FLOAT
            @endtoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state SPECIAL_FLOAT begin
        if quoted && c == quot
            @recordtoken FLOAT|STRING
            @goto QUOTE_END
        elseif c == delim
            if quoted
                @goto STRING
            end
            @recordtoken FLOAT|STRING
            @endtoken
            @goto BEGIN
        elseif c == SP
            if trim && !quoted
                @recordtoken FLOAT|STRING
                @goto FLOAT_SPACE
            end
            @goto STRING
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == LF
            @recordtoken FLOAT|STRING
            @endtoken
            @goto END
        elseif c == CR
            @recordtoken FLOAT|STRING
            @endtoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state FLOAT_SPACE begin
        if c == SP
            @goto FLOAT_SPACE
        elseif c == delim
            @endtoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == LF
            @endtoken
            @goto END
        elseif c == CR
            @endtoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state STRING begin
        if quoted && c == quot
            if qstring
                @recordtoken QSTRING
            else
                @recordtoken STRING
            end
            @goto QUOTE_END
        elseif c == delim
            if quoted
                @goto STRING
            end
            if qstring
                @recordtoken QSTRING
            else
                @recordtoken STRING
            end
            @endtoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == SP
            if trim && !quoted
                if qstring
                    @recordtoken QSTRING
                else
                    @recordtoken STRING
                end
                @goto STRING_SPACE
            end
            @goto STRING
        elseif c == LF
            if qstring
                @recordtoken QSTRING
            else
                @recordtoken STRING
            end
            @endtoken
            @goto END
        elseif c == CR
            if qstring
                @recordtoken QSTRING
            else
                @recordtoken STRING
            end
            @endtoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state STRING_SPACE begin
        if c == SP
            @goto STRING_SPACE
        elseif c == delim
            @endtoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == LF
            @endtoken
            @goto END
        elseif c == CR
            @endtoken
            @goto CR
        else
            @multibytestring
        end
    end

    @state QUOTE_END begin
        if c == delim
            @endtoken
            @goto BEGIN
        elseif c == quot  # e.g. xxx," foo ""bar""",xxx
            qstring = true
            @goto STRING
        elseif c == SP
            if trim
                @goto QUOTE_END_SPACE
            end
            @goto ERROR
        elseif c == LF
            @endtoken
            @goto END
        elseif c == CR
            @endtoken
            @goto CR
        else
            @goto ERROR
        end
    end

    @state QUOTE_END_SPACE begin
        if c == delim
            @endtoken
            @goto BEGIN
        elseif c == SP
            @goto QUOTE_END_SPACE
        elseif c == LF
            @endtoken
            @goto END
        elseif c == CR
            @endtoken
            @goto CR
        else
            @goto ERROR
        end
    end

    @label ERROR
    throw(ReadError("invalid file format at line $(line), char $(repr(c))"))

    @label CR
    if pos + 1 ≤ pos_end && mem[pos + 1] == LF
        pos += 1
    end

    @label END
    if i ≤ ncols
        throw(ReadError("invalid number of columns at line $(line)"))
    end
    return pos
end

# Generated from tools/snoop.jl.
function _precompile_()
    ccall(:jl_generating_output, Cint, ()) == 1 || return nothing
    precompile(Tuple{typeof(TableReader.scanline!), Array{TableReader.Token, 2}, Int64, TranscodingStreams.Memory, Int64, Int64, Int64, UInt8, UInt8, Bool})
    precompile(Tuple{typeof(TableReader.scanheader), TranscodingStreams.Memory, Int64, Int64, UInt8, UInt8, Bool})
    precompile(Tuple{typeof(TableReader.checkformat), Base.IOStream})
    precompile(Tuple{typeof(TableReader.find_first_newline), TranscodingStreams.Memory, Int64})
    precompile(Tuple{typeof(TableReader.check_parser_parameters), Char, Char, Bool, Nothing, Int64})
    precompile(Tuple{typeof(TableReader.checkformat), TranscodingStreams.TranscodingStream{TranscodingStreams.Noop, Base.IOStream}})
    precompile(Tuple{typeof(TableReader.readdlm_internal), TranscodingStreams.TranscodingStream{TranscodingStreams.Noop, Base.IOStream}, TableReader.ParserParameters})
    precompile(Tuple{typeof(TableReader.fillcolumn!), Array{Int64, 1}, Int64, TranscodingStreams.Memory, Array{TableReader.Token, 2}, Int64, UInt8})
    precompile(Tuple{typeof(TableReader.fillcolumn!), Array{String, 1}, Int64, TranscodingStreams.Memory, Array{TableReader.Token, 2}, Int64, UInt8})
    precompile(Tuple{typeof(TableReader.qstring), TranscodingStreams.Memory, Int64, Int64, UInt8})
    precompile(Tuple{typeof(TableReader.aggregate_columns), Array{TableReader.Token, 2}, Int64})
    precompile(Tuple{typeof(TableReader.checkformat), TranscodingStreams.TranscodingStream{TranscodingStreams.Noop, Base.Process}})
    precompile(Tuple{typeof(TableReader.fillcolumn!), Array{Float64, 1}, Int64, TranscodingStreams.Memory, Array{TableReader.Token, 2}, Int64, UInt8})
    precompile(Tuple{typeof(TableReader.wrapstream), Base.IOStream, TableReader.ParserParameters})
    precompile(Tuple{typeof(TableReader.wrapstream), Base.Process, TableReader.ParserParameters})
    precompile(Tuple{typeof(TableReader.checkformat), Base.Process})
    precompile(Tuple{typeof(TableReader.readheader), TranscodingStreams.TranscodingStream{TranscodingStreams.Noop, Base.IOStream}, UInt8, UInt8, Bool})
    precompile(Tuple{typeof(TableReader.readcsv), String})
    precompile(Tuple{typeof(TableReader.readtsv), String})
end
_precompile_()

end # module
