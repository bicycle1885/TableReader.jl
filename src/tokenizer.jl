# Tokenize
# ========

# A set of parser parameters.
struct ParserParameters
    delim::UInt8
    quot::UInt8
    trim::Bool
    skip::Int
    skipblank::Bool
    colnames::Union{Vector{Symbol},Nothing}
    chunksize::Int

    function ParserParameters(delim::Char, quot::Char, trim::Bool, skip::Integer, skipblank::Bool, colnames::Any, chunksize::Integer)
        if delim ∉ ALLOWED_DELIMITERS
            throw(ArgumentError("delimiter $(repr(delim)) is not allowed"))
        elseif quot ∉ ALLOWED_QUOTECHARS
            throw(ArgumentError("quotation character $(repr(quot)) is not allowed"))
        elseif delim == quot
            throw(ArgumentError("delimiter and quotation character cannot be the same character"))
        elseif delim == ' ' && trim
            throw(ArgumentError("delimiting with space and space trimming are exclusive"))
        elseif quot == ' ' && trim
            throw(ArgumentError("quoting with space and space trimming are exclusive"))
        elseif skip < 0
            throw(ArgumentError("skip cannot be negative"))
        elseif chunksize < 0
            throw(ArgumentError("chunk size cannot be negative"))
        elseif chunksize > MAX_TOKEN_START
            throw(ArgumentError("chunk size must be smaller than or equal to $(MAX_TOKEN_START)"))
        end
        if colnames != nothing
            colnames = Symbol.(collect(colnames))
        end
        return new(
            UInt8(delim),
            UInt8(quot),
            trim,
            skip,
            skipblank,
            colnames,
            chunksize,
        )
    end
end


# Token type
# ----------

# token kind
const STRING  = 0b0000
const INTEGER = 0b0001
const FLOAT   = 0b0010
const BOOL    = 0b0100
const QSTRING = 0b1000  # string with quotation marks
const MISSING = 0b1111  # missing can be any data type

const MAX_TOKEN_START = 2^36 - 1
const MAX_TOKEN_LENGTH = 2^24 - 1

struct Token
    # From most significant
    #    4bit: kind (+ missing)
    #   36bit: start position (64 GiB)
    #   24bit: length (16 MiB)
    value::UInt64

    function Token(kind::UInt8, start::Int, len::Int)
        @assert start ≤ MAX_TOKEN_START
        @assert len ≤ MAX_TOKEN_LENGTH
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

function Base.length(token::Token)
    return (token.value & (~UInt64(0) >> 40)) % Int
end


# Line scanner
# ------------

struct ReadError <: Exception
    msg::String
end

function Base.show(io::IO, error::ReadError)
    print(io, summary(error), ": ", error.msg)
end

macro state(name, ex)
    @assert name isa Symbol
    @assert ex isa Expr && ex.head == :block
    quote
        @label $(name)
        #println($(QuoteNode(name)))
        #@show quoted
        pos += 1
        #@assert 1 ≤ pos ≤ lastindex(mem)
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

macro follows(s, opts...)
    @assert s isa String && isascii(s)
    casesensitive = :casesensitive ∈ opts
    i = 0
    foldl(s, init = :(pos + $(sizeof(s)) ≤ pos_end)) do ex, c
        i += 1
        if casesensitive
            :($(ex) && mem[pos+$(i)] == $(UInt8(c)))
        else
            up = UInt8(uppercase(c))
            lo = UInt8(lowercase(c))
            :($(ex) && (mem[pos+$(i)] == $(up) || mem[pos+$(i)] == $(lo)))
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

macro endheadertoken()
    quote
        push!(tokens, token)
        quoted = false
        qstring = false
    end |> esc
end

function scanheader(mem::Memory, lastnl::Int, params::ParserParameters)
    # Check parameters.
    delim, quot, trim = params.delim, params.quot, params.trim
    @assert delim != quot
    @assert !trim || delim != SP
    @assert !trim || quot != SP
    @assert mem[lastnl] == CR || mem[lastnl] == LF

    # Initialize variables.
    tokens = Token[]
    token = TOKEN_NULL
    quoted = false
    qstring = false
    pos = 0
    start = 0
    pos_end = lastnl

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
            @goto LF
        elseif c == CR
            @recordtoken STRING
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
            @goto LF
        elseif c == CR
            @recordtoken STRING
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
            @goto LF
        elseif c == CR
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
            quoted = false
            @goto LF
        elseif c == CR
            quoted = false
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
            quoted = false
            @goto LF
        elseif c == CR
            quoted = false
            @goto CR
        else
            @goto ERROR
        end
    end

    @label ERROR
    throw(ReadError("invalid file header format"))

    @label CR
    if quoted
        throw(ReadError("quoted multiline string is not allowed in the header"))
    elseif pos + 1 ≤ pos_end && mem[pos + 1] == LF
        pos += 1
    end

    @label LF
    if quoted
        throw(ReadError("quoted multiline string is not allowed in the header"))
    end
    @endheadertoken

    @label END
    return pos, tokens
end

macro endtoken()
    quote
        if i > ncols
            msg = "unexpected number of columns at line $(line)"
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
        params::ParserParameters
    )

    # Check parameters.
    delim, quot, trim = params.delim, params.quot, params.trim
    @assert delim != quot
    @assert !trim || delim != SP
    @assert !trim || quot != SP
    @assert mem[lastnl] == CR || mem[lastnl] == LF

    # Initialize variables.
    pos_end = lastnl
    ncols = size(tokens, 1)
    quoted = false
    qstring = false
    token = TOKEN_NULL
    msg = ""  # error message
    start = 0  # the starting position of a token
    i = 1  # the column of a token

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
        elseif (c == UInt8('N') || c == UInt8('n'))
            if @follows("AN")  # case-insensitive
                # NaN
                pos += 2  # for 'A' and 'N'
                @goto SPECIAL_FLOAT
            elseif c == UInt8('N') && @follows("A", casesensitive)
                # NA
                pos += 1  # for 'A'
                @goto NA
            end
            @goto STRING
        elseif c == UInt8('F') || c == UInt8('f')
            if @follows("alse")
                pos += 4
                @goto BOOL
            end
            @goto BOOL
        elseif c == UInt8('T') || c == UInt8('t')
            if @follows("rue")
                pos += 3
                @goto BOOL
            end
            @goto BOOL
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == LF
            if i == ncols
                @recordtoken MISSING
            end
            @goto LF
        elseif c == CR
            if i == ncols
                @recordtoken MISSING
            end
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
            @goto LF
        elseif c == CR
            @recordtoken STRING
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
            @goto LF
        elseif c == CR
            @recordtoken INTEGER|FLOAT
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
            @goto LF
        elseif c == CR
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
            @goto LF
        elseif c == CR
            @recordtoken STRING
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
            @goto LF
        elseif c == CR
            @recordtoken FLOAT
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
            @goto LF
        elseif c == CR
            @recordtoken STRING
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
            @goto LF
        elseif c == CR
            @recordtoken STRING
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
            @goto LF
        elseif c == CR
            @recordtoken FLOAT
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
            @goto LF
        elseif c == CR
            @recordtoken FLOAT|STRING
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
            @goto LF
        elseif c == CR
            @goto CR
        else
            @multibytestring
        end
    end

    @state BOOL begin
        if quoted && c == quot
            @recordtoken BOOL
            @goto QUOTE_END
        elseif c == delim
            if quoted
                @goto STRING
            end
            @recordtoken BOOL
            @endtoken
            @goto BEGIN
        elseif c == SP
            if trim && !quoted
                @recordtoken BOOL
                @goto BOOL_SPACE
            end
            @goto STRING
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == LF
            @recordtoken BOOL
            @goto LF
        elseif c == CR
            @recordtoken BOOL
            @goto CR
        else
            @multibytestring
        end
    end

    @state BOOL_SPACE begin
        if c == SP
            @goto BOOL_SPACE
        elseif c == delim
            @endtoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == LF
            @recordtoken BOOL
            @goto LF
        elseif c == CR
            @recordtoken BOOL
            @goto CR
        else
            @multibytestring
        end
    end

    @state NA begin
        if quoted && c == quot
            @recordtoken MISSING
            @goto QUOTE_END
        elseif c == delim
            if quoted
                @goto STRING
            end
            @recordtoken MISSING
            @endtoken
            @goto BEGIN
        elseif c == SP
            if trim && !quoted
                @recordtoken MISSING
                @goto NA_SPACE
            end
            @goto STRING
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == LF
            @recordtoken MISSING
            @goto LF
        elseif c == CR
            @recordtoken MISSING
            @goto CR
        else
            @multibytestring
        end
    end

    @state NA_SPACE begin
        if c == SP
            @goto NA_SPACE
        elseif c == delim
            @endtoken
            @goto BEGIN
        elseif UInt8('!') ≤ c ≤ UInt8('~')
            @goto STRING
        elseif c == LF
            @recordtoken MISSING
            @goto LF
        elseif c == CR
            @recordtoken MISSING
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
            @goto LF
        elseif c == CR
            if qstring
                @recordtoken QSTRING
            else
                @recordtoken STRING
            end
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
            @goto LF
        elseif c == CR
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
            quoted = false
            @goto LF
        elseif c == CR
            quoted = false
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
            quoted = false
            @goto LF
        elseif c == CR
            quoted = false
            @goto CR
        else
            @goto ERROR
        end
    end

    @label ERROR
    if isempty(msg)
        # default error message
        msg = "invalid file format at line $(line), column $(i) "
        if c ≤ 0x7f  # ASCII
            msg = string(msg, "(found $(repr(Char(c))))")
        else
            msg = string(msg, "(found $(repr(c)))")
        end
    end
    throw(ReadError(msg))

    @label CR  # carriage return
    if pos + 1 ≤ pos_end && mem[pos + 1] == LF
        pos += 1
        # fall through
    elseif quoted
        if pos == lastnl
            # need more data
            return 0, 0
        end
        @goto STRING
    else
        @endtoken
        @goto END
    end

    @label LF  # line feed
    if quoted
        if pos == lastnl
            # need more data
            return 0, 0
        end
        @goto STRING
    else
        @endtoken
        # fall through
    end

    @label END
    return pos, i - 1
end
