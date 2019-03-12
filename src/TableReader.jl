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

const MINIMUM_CHUNK_SIZE = 16 * 2^10  # 16 KiB
const DEFAULT_CHUNK_SIZE =  1 * 2^20  #  1 MiB
const MAX_BUFFERED_ROWS = 1000

# Printable characters
const CHARS_PRINT = ' ':'~'

# Whitelist of delimiters
const ALLOWED_DELIMITERS = tuple(['\t'; ' '; CHARS_PRINT[ispunct.(CHARS_PRINT)]]...)

function check_parser_parameters(delim::Char, quot::Char, trim::Bool, chunksize::Integer)
    if delim ∉ ALLOWED_DELIMITERS
        throw(ArgumentError("delimiter $(repr(delim)) is not allowed"))
    elseif delim == quot
        throw(ArgumentError("delimiter and quote cannot be the same character"))
    elseif delim == ' ' && trim
        throw(ArgumentError("space delimiter and space trimming are exclusive"))
    elseif quot == ' ' && trim
        throw(ArgumentError("space quote and space trimming are exclusive"))
    elseif chunksize < 0
        throw(ArgumentError("chunks size cannot be negative"))
    end
end

"""
    readdlm(filename or IO object;
            delim,
            quot = '"',
            trim = true,
            chunksize = $(DEFAULT_CHUNK_SIZE))

Read a character delimited text file.

`delim` specifies the field delimiter in a line. This must be tab, space, or an
ASCII punctuation character.

`quot` specifies the quotation to enclose a field. This cannot be the same
character as `delim`.

`trim` specifies whether the parser trims space (0x20) characters around a field.
If `trim` is true, `delim` and `quot` cannot be a space character.

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
    if delim === nothing
        delimarg = :(delim::Char)
    else
        delimarg = Expr(:kw, :(delim::Char), delim)
    end
    @eval begin
        function $(fname)(filename::AbstractString;
                          $(delimarg),
                          quot::Char = '"',
                          trim::Bool = true,
                          chunksize::Integer = DEFAULT_CHUNK_SIZE)
            check_parser_parameters(delim, quot, trim, chunksize)
            return open(filename) do file
                if chunksize == 0
                    # without chunking
                    bufsize = DEFAULT_CHUNK_SIZE
                    if endswith(filename, ".gz")
                        data = read(GzipDecompressorStream(file, bufsize = bufsize))
                    elseif endswith(filename, ".zst")
                        data = read(ZstdDecompressorStream(file, bufsize = bufsize))
                    elseif endswith(filename, ".xz")
                        data = read(XzDecompressorStream(file, bufsize = bufsize))
                    else
                        data = read(file)
                    end
                    buffer = Buffer(data)
                    stream = TranscodingStream(Noop(), devnull, State(buffer, buffer))
                else
                    # with chunking
                    bufsize = max(chunksize, MINIMUM_CHUNK_SIZE)
                    if endswith(filename, ".gz")
                        stream = GzipDecompressorStream(file, bufsize = bufsize)
                    elseif endswith(filename, ".zst")
                        stream = ZstdDecompressorStream(file, bufsize = bufsize)
                    elseif endswith(filename, ".xz")
                        stream = XzDecompressorStream(file, bufsize = bufsize)
                    else
                        stream = NoopStream(file, bufsize = bufsize)
                    end
                end
                return readdlm_internal(stream, UInt8(delim), UInt8(quot), trim, chunksize != 0)
            end
        end

        function $(fname)(file::IO;
                          $(delimarg),
                          quot::Char = '"',
                          trim::Bool = true,
                          chunksize::Integer = DEFAULT_CHUNK_SIZE)
            check_parser_parameters(delim, quot, trim, chunksize)
            if chunksize == 0
                buffer = Buffer(read(file))
                stream = TranscodingStream(Noop(), devnull, State(buffer, buffer))
            else
                bufsize = max(chunksize, MINIMUM_CHUNK_SIZE)
                if file isa TranscodingStream
                    stream = file
                else
                    stream = NoopStream(file, bufsize = bufsize)
                end
            end
            return readdlm_internal(stream, UInt8(delim), UInt8(quot), trim, chunksize != 0)
        end
    end
end

function readdlm_internal(stream::TranscodingStream, delim::UInt8, quot::UInt8, trim::Bool, chunking::Bool)
    colnames = readheader(stream, delim, quot, trim)
    ncols = length(colnames)
    if ncols == 0
        return DataFrame()
    end
    tokens = Array{Token}(undef, (ncols, MAX_BUFFERED_ROWS))
    #fill!(tokens, Token(0x00, 0, 0))
    n_chunk_rows = size(tokens, 2)
    columns = Vector[]
    line = 2
    while !eof(stream)
        fillbuffer(stream, eager = true)
        mem = buffermem(stream.state.buffer1)
        lastnl = find_last_newline(mem)
        @assert lastnl > 0  # TODO
        pos = 0
        chunk_begin = line
        if chunking
            while pos < lastnl && line - chunk_begin + 1 ≤ n_chunk_rows
                pos = scanline!(tokens, line - chunk_begin + 1, mem, pos, lastnl, line, delim, quot, trim)
                line += 1
            end
        else
            while pos < lastnl && line - chunk_begin + 1 ≤ n_chunk_rows
                pos = scanline!(tokens, line - chunk_begin + 1, mem, pos, lastnl, line, delim, quot, trim)
                line += 1
                if line - chunk_begin + 1 > n_chunk_rows
                    tokens′ = Array{Token}(undef, (ncols, n_chunk_rows * 2))
                    tokens′[:,1:n_chunk_rows] = tokens
                    tokens = tokens′
                    n_chunk_rows = size(tokens, 2)
                end
            end
        end
        n_new_records = line - chunk_begin
        if isempty(columns)
            resize!(columns, ncols)
            # infer data types of columns
            for i in 1:ncols
                parsable = 0b0111
                hasmissing = false
                @inbounds for j in 1:n_new_records
                    x = kind(tokens[i,j])
                    parsable &= x
                    hasmissing |= (x & 0b1000) != 0
                end
                if (parsable & INTEGER) != 0
                    columns[i] = hasmissing ? Union{Int,Missing}[] : Int[]
                elseif (parsable & FLOAT) != 0
                    columns[i] = hasmissing ? Union{Float64,Missing}[] : Float64[]
                else
                    # fall back to string
                    columns[i] = hasmissing ? Union{String,Missing}[] : String[]
                end
            end
        else
            for i in 1:ncols
                parsable = 0b0111
                hasmissing = false
                @inbounds for j in 1:n_new_records
                    x = kind(tokens[i,j])
                    parsable &= x
                    hasmissing |= (x & 0b1000) != 0
                end
                col = columns[i]
                if col isa Vector{Int} || col isa Vector{Union{Int,Missing}}
                    (parsable & INTEGER) == 0 && throw(ReadError("type guessing failed at column $(i)"))
                elseif col isa Vector{Float64} || col isa Vector{Union{Float64,Missing}}
                    (parsable & FLOAT) == 0 && throw(ReadError("type guessing failed at column $(i)"))
                else
                    @assert col isa Vector{String} || col isa Vector{Union{String,Missing}}
                end
                # allow missing if any
                if col isa Vector{Int} && hasmissing
                    columns[i] = copyto!(Vector{Union{Int,Missing}}(undef, length(col)), col)
                elseif col isa Vector{Float64} && hasmissing
                    columns[i] = copyto!(Vector{Union{Float64,Missing}}(undef, length(col)), col)
                elseif col isa Vector{String} && hasmissing
                    columns[i] = copyto!(Vector{Union{String,Missing}}(undef, length(col)), col)
                end
            end
        end
        for i in 1:ncols
            col = columns[i]
            resize!(col, length(col) + n_new_records)
            fillcolumn!(col, n_new_records, mem, tokens, i, quot)
        end
        skip(stream, pos)
        if !chunking
            @assert eof(stream)
        end
    end
    return DataFrame(columns, colnames)
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

function find_last_newline(mem::Memory)
    i = lastindex(mem)
    #while i > firstindex(mem)
    while i > 0
        @inbounds x = mem[i]
        if x == UInt8('\n')
            break
        end
        i -= 1
    end
    return i
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

function fillcolumn!(col::Vector{String}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int, quot::UInt8)
    @inbounds for i in 1:nvals
        t = tokens[c,i]
        start, length = location(t)
        if kind(t) & QSTRING != 0
            col[end-nvals+i] = qstring(mem, start, length, quot)
        else
            col[end-nvals+i] = unsafe_string(mem.ptr + start - 1, length)
        end
    end
    return col
end

function fillcolumn!(col::Vector{Union{String,Missing}}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int, quot::UInt8)
    last = ""
    @inbounds for i in 1:nvals
        t = tokens[c,i]
        if ismissing(t)
            col[end-nvals+i] = missing
        else
            start, length = location(tokens[c,i])
            if length == sizeof(last) && ccall(:memcmp, Cint, (Ptr{Cvoid}, Ptr{Cvoid}, Csize_t), mem.ptr + start - 1, pointer(last), length) == 0
                # pass
            else
                last = kind(t) & QSTRING != 0 ? qstring(mem, start, length, quot) : unsafe_string(mem.ptr + start - 1, length)
            end
            col[end-nvals+i] = last
        end
    end
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
    # TODO: be more careful
    header = readline(stream)
    if all(isequal(' '), header)
        return Symbol[]
    end
    return [Symbol(trim ? strip(strip1(x, quot)) : strip1(x, quot)) for x in split(header, Char(delim))]
end

function strip1(s::AbstractString, c::UInt8)
    char = Char(c)
    if isempty(s)
        return s
    end
    return chop(s, head = startswith(s, char), tail = endswith(s, char))
end

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
        if pos > pos_end
            @goto END
        end
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
            if pos + 1 ≤ pos_end && (0b10_000000 ≤ mem[pos+1] ≤ 0b10_111111)
                pos += 1
                @goto STRING
            end
        elseif 0b1110_0000 ≤ c ≤ 0b1110_1111
            if pos + 2 ≤ pos_end && (0b10_000000 ≤ mem[pos+1] ≤ 0b10_111111) && (0b10_000000 ≤ mem[pos+2] ≤ 0b10_111111)
                pos += 2
                @goto STRING
            end
        elseif 0b11110_000 ≤ c ≤ 0b11110_111
            if pos + 3 ≤ pos_end && (0b10_000000 ≤ mem[pos+1] ≤ 0b10_111111) && (0b10_000000 ≤ mem[pos+2] ≤ 0b10_111111) && (0b10_000000 ≤ mem[pos+3] ≤ 0b10_111111)
                pos += 3
                @goto STRING
            end
        else
            @goto ERROR
        end
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
    @assert !trim || delim != UInt8(' ')
    @assert !trim || quot != UInt8(' ')

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
        elseif c == UInt8(' ')
            if trim && !quoted
                @goto BEGIN
            else
                @goto STRING
            end
        elseif c == UInt8('.')
            @goto DOT
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == UInt8('\n')
            if i == ncols
                @recordtoken MISSING
            end
            @endtoken
            @goto END
        elseif c == UInt8('\r')
            if i == ncols
                @recordtoken MISSING
            end
            @endtoken
            @goto CR_LF
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
        elseif c == UInt8(' ')
            if trim && !quoted
                @recordtoken STRING
                @goto STRING_SPACE
            else
                @goto STRING
            end
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == UInt8('\n')
            @recordtoken STRING
            @endtoken
            @goto END
        elseif c == UInt8('\r')
            @recordtoken STRING
            @endtoken
            @goto CR_LF
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
        elseif c == UInt8(' ')
            if trim && !quoted
                @recordtoken INTEGER|FLOAT
                @goto INTEGER_SPACE
            else
                @goto STRING
            end
        elseif c == UInt8('e') || c == UInt8('E')
            @goto EXPONENT
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == UInt8('\n')
            @recordtoken INTEGER|FLOAT
            @endtoken
            @goto END
        elseif c == UInt8('\r')
            @recordtoken INTEGER|FLOAT
            @endtoken
            @goto CR_LF
        else
            @multibytestring
        end
    end

    @state INTEGER_SPACE begin
        if c == UInt8(' ')
            @goto INTEGER_SPACE
        elseif c == delim
            @endtoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == UInt8('\n')
            @endtoken
            @goto END
        elseif c == UInt8('\r')
            @endtoken
            @goto CR_LF
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
        elseif c == UInt8(' ')
            if trim && !quoted
                @recordtoken STRING
                @goto STRING_SPACE
            else
                @goto STRING
            end
        elseif c == UInt8('\n')
            @recordtoken STRING
            @endtoken
            @goto END
        elseif c == UInt8('\r')
            @recordtoken STRING
            @endtoken
            @goto CR_LF
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
        elseif c == UInt8(' ')
            if trim && !quoted
                @recordtoken FLOAT
                @goto POINT_FLOAT_SPACE
            else
                @goto STRING
            end
        elseif c == UInt8('\n')
            @recordtoken FLOAT
            @endtoken
            @goto END
        elseif c == UInt8('\r')
            @recordtoken FLOAT
            @endtoken
            @goto CR_LF
        else
            @multibytestring
        end
    end

    @state POINT_FLOAT_SPACE begin
        if c == UInt8(' ')
            @goto POINT_FLOAT_SPACE
        elseif c == delim
            @recordtoken FLOAT
            @endtoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == UInt8('\n')
            @endtoken
            @goto END
        elseif c == UInt8('\r')
            @endtoken
            @goto CR_LF
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
        elseif c == UInt8(' ')
            if trim && !quoted
                @recordtoken STRING
                @goto STRING_SPACE
            else
                @goto STRING
            end
        elseif c == UInt8('\n')
            @recordtoken STRING
            @endtoken
            @goto END
        elseif c == UInt8('\r')
            @recordtoken STRING
            @endtoken
            @goto CR_LF
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
        elseif c == UInt8(' ')
            if trim && !quoted
                @recordtoken STRING
                @goto STRING_SPACE
            else
                @goto STRING
            end
        elseif c == UInt8('\n')
            @recordtoken STRING
            @endtoken
            @goto END
        elseif c == UInt8('\r')
            @recordtoken STRING
            @endtoken
            @goto CR_LF
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
        elseif c == UInt8(' ')
            if trim && !quoted
                @recordtoken FLOAT
                @goto EXPONENT_FLOAT_SPACE
            else
                @goto STRING
            end
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == UInt8('\n')
            @recordtoken FLOAT
            @endtoken
            @goto END
        elseif c == UInt8('\r')
            @recordtoken FLOAT
            @endtoken
            @goto CR_LF
        else
            @multibytestring
        end
    end

    @state EXPONENT_FLOAT_SPACE begin
        if c == UInt8(' ')
            @goto EXPONENT_FLOAT_SPACE
        elseif c == delim
            @endtoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == UInt8('\n')
            @endtoken
            @goto END
        elseif c == UInt8('\r')
            @endtoken
            @goto CR_LF
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
        elseif c == UInt8(' ')
            if trim && !quoted
                if qstring
                    @recordtoken QSTRING
                else
                    @recordtoken STRING
                end
                @goto STRING_SPACE
            else
                @goto STRING
            end
        elseif c == UInt8('\n')
            if qstring
                @recordtoken QSTRING
            else
                @recordtoken STRING
            end
            @recordtoken STRING
            @endtoken
            @goto END
        elseif c == UInt8('\r')
            if qstring
                @recordtoken QSTRING
            else
                @recordtoken STRING
            end
            @endtoken
            @goto CR_LF
        else
            @multibytestring
        end
    end

    @state STRING_SPACE begin
        if c == UInt8(' ')
            @goto STRING_SPACE
        elseif c == delim
            @endtoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == UInt8('\n')
            @endtoken
            @goto END
        elseif c == UInt8('\r')
            @endtoken
            @goto CR_LF
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
        elseif c == UInt8(' ')
            if trim
                @goto QUOTE_END_SPACE
            else
                @goto ERROR
            end
        elseif c == UInt8('\n')
            @endtoken
            @goto END
        elseif c == UInt8('\r')
            @endtoken
            @goto CR_LF
        else
            @goto ERROR
        end
    end

    @state QUOTE_END_SPACE begin
        if c == delim
            @endtoken
            @goto BEGIN
        elseif c == UInt8(' ')
            @goto QUOTE_END_SPACE
        elseif c == UInt8('\n')
            @endtoken
            @goto END
        elseif c == UInt8('\r')
            @endtoken
            @goto CR_LF
        else
            @goto ERROR
        end
    end

    @state CR_LF begin
        if c == UInt8('\n')
            @goto END
        else
            @goto ERROR
        end
    end

    @label ERROR
    throw(ReadError("invalid file format at line $(line), char $(repr(c))"))

    @label END
    if i ≤ ncols
        throw(ReadError("invalid number of columns at line $(line)"))
    end
    return pos
end

precompile(Tuple{typeof(scanline!), Matrix{Token}, Int, Memory, Int, Int, Int, UInt8, UInt8, Bool})

end # module
