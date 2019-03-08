module TableReader

export readtsv

using DataFrames:
    DataFrame
using TranscodingStreams:
    TranscodingStream,
    NoopStream,
    Memory,
    buffermem

function readtsv(filename::AbstractString)
    return open(readtsv, filename)
end

function readtsv(file::IO)
    return readtsv(NoopStream(file, bufsize = 128 * 2^20))
end

function readtsv(stream::TranscodingStream)
    header = readline(stream)
    colnames = Symbol.(split(header, '\t'))
    ncols = length(colnames)
    columns = [sizehint!(Any[], 1000) for _ in 1:ncols]
    tokens = Vector{UnitRange{Int}}(undef, ncols)
    state = ParserState(stream, 1, columns, tokens)
    delim = UInt8('\t')
    while !eof(stream)
        mem = buffermem(stream.state.buffer1)
        Δ = scanline!(state, mem, delim)
        if Δ == 0
            # TODO
            @assert false
        end
        for i in 1:length(tokens)
            val = parse_integer(mem, tokens[i])
            push!(columns[i], val)
        end
        skip(stream, Δ)
    end
    return DataFrame(columns, colnames)
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
    columns::Vector{Vector}
    tokens::Vector{UnitRange{Int}}
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

# Scan a line in mem; mem must include one or more lines.
function scanline!(state::ParserState, mem::Memory, delim::UInt8)
    @assert delim ∈ (UInt8('\t'), UInt8(';'), UInt8('|'),)
    pos = 0
    pos_end = lastindex(mem)
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
        state.tokens[i] = token:pos-1
        i += 1
        @goto BEGIN
    elseif c == UInt8('\n')
        state.tokens[i] = token:pos-1
        @goto END
    end
    @goto ERROR

    @state INTEGER
    if UInt8('0') ≤ c ≤ UInt8('9')
        @goto INTEGER
    elseif c == delim
        state.tokens[i] = token:pos-1
        i += 1
        @goto BEGIN
    elseif c == UInt8(' ')
        state.tokens[i] = token:pos-1
        @goto INTEGER_SPACE
    elseif UInt8(' ') ≤ c ≤ UInt8('~')
        @goto STRING
    elseif c == UInt8('\n')
        state.tokens[i] = token:pos-1
        @goto END
    end
    @goto ERROR

    @state INTEGER_SPACE
    if c == UInt8(' ')
        @goto INTEGER_SPACE
    elseif c == delim
        i += 1
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
        state.tokens[i] = token:pos-1
        @goto BEGIN
    elseif c == UInt8('\n')
        state.tokens[i] = token:pos-1
        @goto END
    end
    @goto ERROR

    @label ERROR
    throw(ReadError("invalid file format at line $(state.line), char $(repr(c))"))

    @label END
    state.line += 1
    return pos
end

end # module
