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
    fillbuffer,
    buffermem
using CodecZlib:
    GzipDecompressorStream
using CodecZstd:
    ZstdDecompressorStream
using CodecXz:
    XzDecompressorStream

const DEFAULT_BUFFER_SIZE = 8 * 2^20  # 8 MiB
const MAX_BUFFERED_ROWS = 100

# Printable characters
const CHARS_PRINT = ' ':'~'

# Whitelist of delimiters
const ALLOWED_DELIMITERS = tuple(['\t'; ' '; CHARS_PRINT[ispunct.(CHARS_PRINT)]]...)

function check_parser_parameters(delim::Char, quot::Char, trim::Bool)
    if delim ∉ ALLOWED_DELIMITERS
        throw(ArgumentError("delimiter $(repr(delim)) is not allowed"))
    elseif delim == quot
        throw(ArgumentError("delimiter and quote cannot be the same character"))
    elseif delim == ' ' && trim
        throw(ArgumentError("space delimiter and space trimming are exclusive"))
    elseif quot == ' ' && trim
        throw(ArgumentError("space quote and space trimming are exclusive"))
    end
end

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
                          bufsize::Integer = DEFAULT_BUFFER_SIZE)
            check_parser_parameters(delim, quot, trim)
            return open(filename) do file
                if endswith(filename, ".gz")
                    file = GzipDecompressorStream(file, bufsize = bufsize)
                elseif endswith(filename, ".zst")
                    file = ZstdDecompressorStream(file, bufsize = bufsize)
                elseif endswith(filename, ".xz")
                    file = XzDecompressorStream(file, bufsize = bufsize)
                else
                    file = NoopStream(file, bufsize = bufsize)
                end
                return readdlm_internal(file, UInt8(delim), UInt8(quot), trim)
            end
        end

        function $(fname)(file::IO;
                          $(delimarg),
                          quot::Char = '"',
                          trim::Bool = true,
                          bufsize::Integer = DEFAULT_BUFFER_SIZE)
            check_parser_parameters(delim, quot, trim)
            if !(file isa TranscodingStream)
                file = NoopStream(file, bufsize = bufsize)
            end
            return readdlm_internal(file, UInt8(delim), UInt8(quot), trim)
        end
    end
end

function readdlm_internal(stream::TranscodingStream, delim::UInt8, quot::UInt8, trim::Bool)
    colnames = readheader(stream, delim, quot, trim)
    ncols = length(colnames)
    if ncols == 0
        return DataFrame()
    end
    tokens = Array{Token}(undef, (ncols, MAX_BUFFERED_ROWS))
    #fill!(tokens, Token(0x00, 0, 0))
    n_block_rows = size(tokens, 2)
    columns = Vector[]
    line = 2
    while !eof(stream)
        fillbuffer(stream, eager = true)
        mem = buffermem(stream.state.buffer1)
        lastnl = find_last_newline(mem)
        @assert lastnl > 0  # TODO
        pos = 0
        block_begin = line
        while pos < lastnl && line - block_begin + 1 ≤ n_block_rows
            pos = scanline!(tokens, line - block_begin + 1, mem, pos, lastnl, line, delim, quot, trim)
            line += 1
        end
        n_new_records = line - block_begin
        if isempty(columns)
            resize!(columns, ncols)
            # infer data types of columns
            for i in 1:ncols
                parsable = 0b0111
                hasmissing = false
                for j in 1:n_new_records
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
                for j in 1:n_new_records
                    x = kind(tokens[i,j])
                    parsable &= x
                    hasmissing |= (x & 0b1000) != 0
                end
                col = columns[i]
                if col isa Vector{Int} || col isa Vector{Union{Int,Missing}}
                    (parsable & INTEGER) == 0 && throw(ReadError("type guessing failed"))
                elseif col isa Vector{Float64} || col isa Vector{Union{Float64,Missing}}
                    (parsable & FLOAT) == 0 && throw(ReadError("type guessing failed"))
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
            fillcolumn!(col, n_new_records, mem, tokens, i)
        end
        skip(stream, pos)
    end
    return DataFrame(columns, colnames)
end

# field kind
const STRING  = 0b0000
const INTEGER = 0b0001
const FLOAT   = 0b0010
const MISSING = 0b1111  # missing can be any data type

struct Token
    # From most significant
    #    4bit: kind (+ missing)
    #   30bit: start positin
    #   30bit: end position
    value::UInt64

    function Token(kind::UInt8, start::Int, stop::Int)
        return new((UInt64(kind) << 60) | (UInt64(start) << 30) | UInt64(stop))
    end
end

function kind(token::Token)
    return (token.value >> 60) % UInt8
end

function ismissing(token::Token)
    return (token.value & (UInt64(1) << 63)) != 0
end

function range(token::Token)
    x = token.value & (~UInt64(0) >> 4)
    return (x >> 30) % Int : (x & (~UInt64(0) >> 34)) % Int
end

function bounds(token::Token)
    x = token.value & (~UInt64(0) >> 4)
    return (x >> 30) % Int, (x & (~UInt64(0) >> 34)) % Int
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

function fillcolumn!(col::Vector{Int}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int)
    for i in 1:nvals
        start, stop = bounds(tokens[c,i])
        col[end-nvals+i] = parse_integer(mem, start, stop)
    end
    return col
end

function fillcolumn!(col::Vector{Union{Int,Missing}}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int)
    for i in 1:nvals
        t = tokens[c,i]
        if ismissing(t)
            col[end-nvals+i] = missing
        else
            start, stop = bounds(t)
            col[end-nvals+i] = parse_integer(mem, start, stop)
        end
    end
    return col
end

@inline function parse_integer(mem::Memory, start::Int, stop::Int)
    i = start
    b = mem[start]
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

function fillcolumn!(col::Vector{Float64}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int)
    for i in 1:nvals
        start, stop = bounds(tokens[c,i])
        col[end-nvals+i] = parse_float(mem, start, stop)
    end
    return col
end

function fillcolumn!(col::Vector{Union{Float64,Missing}}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int)
    for i in 1:nvals
        t = tokens[c,i]
        if ismissing(t)
            col[end-nvals+i] = missing
        else
            start, stop = bounds(t)
            col[end-nvals+i] = parse_float(mem, start, stop)
        end
    end
    return col
end

@inline function parse_float(mem::Memory, start::Int, stop::Int)
    hasvalue, val = ccall(:jl_try_substrtod, Tuple{Bool,Float64}, (Ptr{UInt8}, Csize_t, Csize_t), mem.ptr, start-1, stop - start + 1)
    @assert hasvalue
    return val
end

function fillcolumn!(col::Vector{String}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int)
    for i in 1:nvals
        start, stop = bounds(tokens[c,i])
        col[end-nvals+i] = unsafe_string(mem.ptr + start - 1, stop - start + 1)
    end
    return col
end

function fillcolumn!(col::Vector{Union{String,Missing}}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int)
    for i in 1:nvals
        t = tokens[c,i]
        if ismissing(t)
            col[end-nvals+i] = missing
        else
            start, stop = bounds(tokens[c,i])
            col[end-nvals+i] = unsafe_string(mem.ptr + start - 1, stop - start + 1)
        end
    end
    return col
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
    esc(quote
        @label $(name)
        pos += 1
        if pos > pos_end
            @goto END
        end
        @inbounds c = mem[pos]
        $(ex)
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
        end
        @goto ERROR
    end)
end

macro begintoken()
    esc(:(token = pos))
end

macro recordtoken(kind)
    quote
        if i > ncols
            @goto ERROR
        end
        tokens[i,row] = Token($(kind), token, pos - 1)
    end |> esc
end

macro endtoken()
    esc(:(i += 1))
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
    token = 0  # the starting position of a token
    i = 1  # the current token

    @state BEGIN begin
        @begintoken
        if c == quot
            if quoted
                @recordtoken MISSING
                @endtoken
                quoted = false
            else
                quoted = true
            end
            @goto BEGIN
        elseif c == delim
            @recordtoken MISSING
            @endtoken
            @goto BEGIN
        elseif c == UInt8('-') || c == UInt8('+')
            @goto SIGN
        elseif UInt8('0') ≤ c ≤ UInt8('9')
            @goto INTEGER
        elseif c == UInt8(' ')
            if trim
                @goto BEGIN
            else
                @goto STRING
            end
        elseif c == UInt8('.')
            @goto DOT
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == UInt8('\n')
            if i == ncols  # TODO
                @recordtoken MISSING
            end
            @endtoken
            @goto END
        end
    end

    @state SIGN begin
        if quoted && c == quot
            @recordtoken STRING
            @endtoken
            quoted = false
            @goto QUOTE_END
        elseif c == delim
            @recordtoken STRING
            @endtoken
            @goto BEGIN
        elseif UInt8('0') ≤ c ≤ UInt8('9')
            @goto INTEGER
        elseif c == UInt8('.')
            @goto DOT
        elseif c == UInt8(' ')
            if trim
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
        end
    end

    @state INTEGER begin
        if quoted && c == quot
            @recordtoken INTEGER|FLOAT
            @endtoken
            quoted = false
            @goto QUOTE_END
        elseif c == delim
            @recordtoken INTEGER|FLOAT
            @endtoken
            @goto BEGIN
        elseif UInt8('0') ≤ c ≤ UInt8('9')
            @goto INTEGER
        elseif c == UInt8('.')
            @goto POINT_FLOAT
        elseif c == UInt8(' ')
            if trim
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
        end
    end

    @state INTEGER_SPACE begin
        if quoted && c == quot
            @recordtoken STRING
            @endtoken
            quoted = false
            @goto QUOTE_END
        elseif c == UInt8(' ')
            @goto INTEGER_SPACE
        elseif c == delim
            @endtoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == UInt8('\n')
            @endtoken
            @goto END
        end
    end

    @state DOT begin
        if quoted && c == quot
            @recordtoken STRING
            @endtoken
            quoted = false
            @goto QUOTE_END
        elseif c == delim
            @recordtoken STRING
            @endtoken
            @goto BEGIN
        elseif UInt8('0') ≤ c ≤ UInt8('9')
            @goto POINT_FLOAT
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == UInt8(' ')
            if trim
                @recordtoken STRING
                @goto STRING_SPACE
            else
                @goto STRING
            end
        elseif c == UInt8('\n')
            @recordtoken STRING
            @endtoken
            @goto END
        end
    end

    @state POINT_FLOAT begin
        if quoted && c == quot
            @recordtoken FLOAT
            @endtoken
            quoted = false
            @goto QUOTE_END
        elseif c == delim
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
            if trim
                @recordtoken FLOAT
                @goto POINT_FLOAT_SPACE
            else
                @goto STRING
            end
        elseif c == UInt8('\n')
            @recordtoken FLOAT
            @endtoken
            @goto END
        end
    end

    @state POINT_FLOAT_SPACE begin
        if quoted && c == quot
            @recordtoken STRING
            @endtoken
            quoted = false
            @goto QUOTE_END
        elseif c == UInt8(' ')
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
        end
    end

    @state EXPONENT begin
        if quoted && c == quot
            @recordtoken STRING
            @endtoken
            quoted = false
            @goto QUOTE_END
        elseif c == delim
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
            if trim
                @recordtoken STRING
                @goto STRING_SPACE
            else
                @goto STRING
            end
        elseif c == UInt8('\n')
            @recordtoken STRING
            @endtoken
            @goto END
        end
    end

    @state EXPONENT_SIGN begin
        if quoted && c == quot
            @recordtoken STRING
            @endtoken
            quoted = false
            @goto QUOTE_END
        elseif c == delim
            @recordtoken STRING
            @endtoken
            @goto BEGIN
        elseif UInt8('0') ≤ c ≤ UInt8('9')
            @goto EXPONENT_FLOAT
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == UInt8(' ')
            if trim
                @recordtoken STRING
                @goto STRING_SPACE
            else
                @goto STRING
            end
        elseif c == UInt8('\n')
            @recordtoken STRING
            @endtoken
            @goto END
        end
    end

    @state EXPONENT_FLOAT begin
        if quoted && c == quot
            @recordtoken FLOAT
            @endtoken
            quoted = false
            @goto QUOTE_END
        elseif c == delim
            @recordtoken FLOAT
            @endtoken
            @goto BEGIN
        elseif UInt8('0') ≤ c ≤ UInt8('9')
            @goto EXPONENT_FLOAT
        elseif c == UInt8(' ')
            if trim
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
        end
    end

    @state EXPONENT_FLOAT_SPACE begin
        if quoted && c == quot
            @recordtoken STRING
            @endtoken
            quoted = false
            @goto QUOTE_END
        elseif c == UInt8(' ')
            @goto EXPONENT_FLOAT_SPACE
        elseif c == delim
            @endtoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == UInt8('\n')
            @endtoken
            @goto END
        end
    end

    @state STRING begin
        if quoted && c == quot
            @recordtoken STRING
            @endtoken
            quoted = false
            @goto QUOTE_END
        elseif c == delim
            @recordtoken STRING
            @endtoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == UInt8(' ')
            if trim
                @recordtoken STRING
                @goto STRING_SPACE
            else
                @goto STRING
            end
        elseif c == UInt8('\n')
            @recordtoken STRING
            @endtoken
            @goto END
        end
    end

    @state STRING_SPACE begin
        if quoted && c == quot
            @recordtoken STRING
            @endtoken
            quoted = false
            @goto QUOTE_END
        elseif c == UInt8(' ')
            @goto STRING_SPACE
        elseif c == delim
            @endtoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == UInt8('\n')
            @endtoken
            @goto END
        end
    end

    @state QUOTE_END begin
        if c == delim
            @goto BEGIN
        elseif c == UInt8(' ')
            if trim
                @goto QUOTE_END
            else
                @goto ERROR
            end
        elseif c == UInt8('\n')
            @endtoken
            @goto END
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

end # module
