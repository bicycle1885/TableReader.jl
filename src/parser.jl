# Parser
# ======

# Integer
# -------

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
    @inbounds b = mem[i]
    negative = false
    if b == UInt8('-')
        negative = true
        i += 1
    elseif b == UInt8('+')
        i += 1
    end
    n::Int = 0
    while i ≤ stop
        @inbounds b = mem[i]
        n = 10n + (b - UInt8('0'))
        i += 1
    end
    return negative ? -n : n
end


# Float
# -----

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
    return ccall(:jl_strtod_c, Cdouble, (Ptr{UInt8}, Ptr{Cvoid}), mem.ptr + start - 1, C_NULL)
end


function fillcolumn!(col::Vector{Bool}, nvals::Int, mem::Memory, tokens::Matrix{Token}, c::Int, quot::UInt8)
    @inbounds for i in 1:nvals
        start, length = location(tokens[c,i])
        col[end-nvals+i] = parse_bool(mem, start, length)
    end
    return col
end

@inline function parse_bool(mem::Memory, start::Int, length::Int)
    c = mem[start] 
    return (c == UInt8('f') || c == UInt8('F')) ? false : true
end


# String
# ------

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
            s = allocate!(cache, mem.ptr + start - 1, length)
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
                s = allocate!(cache, mem.ptr + start - 1, length)
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


# Date
# ----

const DATE_REGEX = r"^\d{4}-\d{2}-\d{2}$"

function parse_date(s::String)
    return Date(s, dateformat"y-m-d")
end

function parse_date(col::Vector{String})
    out = Vector{Date}(undef, length(col))
    for i in 1:length(col)
        out[i] = parse_date(col[i])
    end
    return out
end

function parse_date(col::Vector{Union{String,Missing}})
    out = Vector{Union{String,Missing}}(undef, length(col))
    for i in 1:length(col)
        x = col[i]
        if x === missing
            out[i] = missing
        else
            out[i] = parse_date(x)
        end
    end
    return out
end

function is_date_like(col::Vector{<:Union{String,Missing}})
    i = 1
    n = 0
    while i ≤ length(col) && n < 3
        x = col[i]
        if x isa String
            if !occursin(DATE_REGEX, x)
                return false
            end
            n += 1
        end
        i += 1
    end
    return n > 0
end


# DateTime
# --------

const DATETIME_REGEX = r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(:?\.\d+)?$"

function parse_datetime(s::String, hasT::Bool)
    if hasT
        return DateTime(s, dateformat"y-m-dTH:M:S.s")
    else
        return DateTime(s, dateformat"y-m-d H:M:S.s")
    end
end

function is_datetime_like(col::Vector{<:Union{String,Missing}})
    # Check if the first three strings (if any) are datetime-like.
    i = 1
    n = 0
    while i ≤ length(col) && n < 3
        x = col[i]
        if x isa String
            if !occursin(DATETIME_REGEX, x)
                return false
            end
            n += 1
        end
        i += 1
    end
    return n > 0
end

function parse_datetime(col::Vector{String}, hasT::Bool)
    out = Vector{DateTime}(undef, length(col))
    for i in 1:length(col)
        out[i] = parse_datetime(col[i], hasT)
    end
    return out
end

function parse_datetime(col::Vector{Union{String,Missing}}, hasT::Bool)
    out = Vector{Union{DateTime,Missing}}(undef, length(col))
    for i in 1:length(col)
        x = col[i]
        if x === missing
            out[i] = missing
        else
            out[i] = parse_datetime(x, hasT)
        end
    end
    return out
end
