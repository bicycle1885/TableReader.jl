module TableReader

export readtsv

using DataFrames:
    DataFrame
using TranscodingStreams:
    TranscodingStream,
    NoopStream,
    Memory,
    buffermem,
    fillbuffer

const DEFAULT_BUFFER_SIZE = 8 * 2^20  # 8 MiB
const MAX_BUFFERED_ROWS = 100

function readtsv(
        filename::AbstractString,
        bufsize::Integer = DEFAULT_BUFFER_SIZE,
    )
    return open(readtsv, filename, bufsize = bufsize)
end

function readtsv(
        file::IO;
        bufsize::Integer = DEFAULT_BUFFER_SIZE,
    )
    return readtsv(NoopStream(file, bufsize = DEFAULT_BUFFER_SIZE))
end

const STRING = UInt8(0)
const INTEGER = UInt8(1) << 0
#const FLOAT   = UInt8(1) << 1
#const BOOL    = UInt8(1) << 2

struct Token
    # From most significant
    #    4bit: kind
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

function range(token::Token)
    x = token.value & (~UInt64(0) >> 4)
    return (x >> 30) % Int : (x & (~UInt64(0) >> 34)) % Int
end

function readtsv(stream::TranscodingStream)
    delim = UInt8('\t')
    colnames = readheader(stream, delim)
    ncols = length(colnames)
    @assert ncols > 0
    fillbuffer(stream)
    tokens = Array{Token}(undef, (ncols, MAX_BUFFERED_ROWS))
    #fill!(tokens, Token(0x00, 0, 0))
    n_block_rows = size(tokens, 2)
    columns = Vector[]
    line = 2
    while !eof(stream)
        mem = buffermem(stream.state.buffer1)
        lastnl = find_last_newline(mem)
        pos = 0
        block_begin = line
        while pos < lastnl && line - block_begin + 1 ≤ n_block_rows
            pos = scanline!(tokens, line - block_begin + 1, mem, pos, lastnl, line, delim)
            line += 1
        end
        n_new_records = line - block_begin
        if isempty(columns)
            resize!(columns, ncols)
            # infer data types of columns
            for i in 1:ncols
                parsable = INTEGER
                for j in 1:n_new_records
                    parsable &= kind(tokens[i,j])
                end
                if (parsable & INTEGER) != 0
                    columns[i] = Int[]
                else
                    # fall back to string
                    columns[i] = String[]
                end
            end
        end
        # TODO: check that columns are really parsable
        for i in 1:ncols
            col = columns[i]
            resize!(col, length(col) + n_new_records)
            fillcolumn!(col, n_new_records, mem, tokens, i)
        end
        skip(stream, pos)
    end
    return DataFrame(columns, colnames)
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
        col[end-nvals+i] = parse_integer(mem, range(tokens[c,i]))
    end
    return col
end

function fillcolumn!(col::Vector{String}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int)
    for i in 1:nvals
        r = range(tokens[c,i])
        col[end-nvals+i] = unsafe_string(mem.ptr + first(r) - 1, length(r))
    end
    return col
end

# Read header and return column names.
function readheader(stream::TranscodingStream, delim::UInt8)
    header = readline(stream)
    return Symbol.(split(header, Char(delim)))
end

@inline function parse_integer(mem, range)
    n::Int = 0
    for i in range
        @inbounds b = mem[i]
        n = 10n + (b - UInt8('0'))
    end
    return n
end

mutable struct ParserState
    stream::TranscodingStream
    line::Int
end

struct ReadError <: Exception
    msg::String
end

macro state(name)
    esc(quote
        @label $(name)
        pos += 1
        if pos > pos_end
            @goto END
        end
        @inbounds c = mem[pos]
        #@show Char(c)
    end)
end

macro begintoken()
    esc(quote
        token = pos
    end)
end

macro recordtoken(kind)
    esc(quote
        @assert token > 0
        tokens[i,row] = Token($(kind), token, pos - 1)
    end)
end

macro endtoken()
    esc(quote
        i += 1
    end)
end

# Scan a line in mem; mem must include one or more lines.
function scanline!(tokens::Matrix{Token}, row::Int,
                   mem::Memory, pos::Int, lastnl::Int, line::Int, delim::UInt8)
    @assert delim ∈ (UInt8('\t'), UInt8(';'), UInt8('|'),)
    pos_end = lastnl
    token = 0  # the starting position of a token
    i = 1  # the current token

    @state BEGIN
    if c == UInt8('-') || c == UInt8('+')
        @begintoken
        @goto SIGN
    elseif UInt8('0') ≤ c ≤ UInt8('9')
        @begintoken
        @goto INTEGER
    elseif c == UInt8(' ')
        @goto BEGIN
    elseif UInt8('!') ≤ c ≤ UInt8('~')
        @begintoken
        @goto STRING
    elseif c == UInt8('\n')
        @goto END
    end
    @goto ERROR

    @state SIGN
    if UInt8('0') ≤ c ≤ UInt8('9')
        @goto INTEGER
    elseif UInt8(' ') ≤ c ≤ UInt8('~')
        @goto STRING
    elseif c == delim
        @recordtoken STRING
        @endtoken
        @goto BEGIN
    elseif c == UInt8('\n')
        @recordtoken STRING
        @goto END
    end
    @goto ERROR

    @state INTEGER
    if UInt8('0') ≤ c ≤ UInt8('9')
        @goto INTEGER
    elseif c == delim
        @recordtoken INTEGER
        @endtoken
        @goto BEGIN
    elseif c == UInt8(' ')
        @recordtoken INTEGER
        @goto INTEGER_SPACE
    elseif UInt8(' ') ≤ c ≤ UInt8('~')
        @goto STRING
    elseif c == UInt8('\n')
        @recordtoken INTEGER
        @goto END
    end
    @goto ERROR

    @state INTEGER_SPACE
    if c == UInt8(' ')
        @goto INTEGER_SPACE
    elseif c == delim
        @endtoken
        @goto BEGIN
    elseif UInt8(' ') ≤ c ≤ UInt8('~')
        @goto STRING
    elseif c == UInt8('\n')
        @goto END
    end
    @goto ERROR

    @state STRING
    if UInt8(' ') ≤ c ≤ UInt8('~')
        @goto STRING
    elseif c == delim
        @recordtoken STRING
        @endtoken
        @goto BEGIN
    elseif c == UInt8('\n')
        @recordtoken STRING
        @endtoken
        @goto END
    end
    @goto ERROR

    @label ERROR
    throw(ReadError("invalid file format at line $(line), char $(repr(c))"))

    @label END
    return pos
end

end # module
