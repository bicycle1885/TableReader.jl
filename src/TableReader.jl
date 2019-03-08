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
    line::Int
end

macro state(name)
    esc(quote
        @label $(name)
        pos += 1
        if pos > pos_end
            @goto ENDFILE
        end
        @inbounds c = mem[pos]
        #@show Char(c)
    end)
end

# Scan a line in mem; mem must include one or more lines.
function scanline!(state::ParserState, mem::Memory, delim::UInt8)
    pos = 0
    pos_end = lastindex(mem)
    pos_token = 0
    i = 1

    @state STATE_BEGIN
    if UInt8('0') ≤ c ≤ UInt8('9')
        pos_token = pos
        @goto STATE_INTEGER
    elseif c == UInt8('\n')
        @goto ENDLINE
    end
    @goto ERROR

    @state STATE_INTEGER
    if UInt8('0') ≤ c ≤ UInt8('9')
        @goto STATE_INTEGER
    elseif c == delim
        state.tokens[i] = pos_token:pos-1
        i += 1
        @goto STATE_DELIM
    elseif c == UInt8('\n')
        state.tokens[i] = pos_token:pos-1
        i += 1
        @goto ENDLINE
    end
    @goto ERROR

    @state STATE_DELIM
    if UInt8('0') ≤ c ≤ UInt8('9')
        pos_token = pos
        @goto STATE_INTEGER
    end
    @goto ERROR

    @label ERROR
    throw(ReadError("invalid file format at line $(state.line)", state.line))

    @label ENDFILE
    return 0

    @label ENDLINE
    state.line += 1
    return pos
end

end # module
