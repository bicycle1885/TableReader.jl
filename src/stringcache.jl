# String Cache
# ------------

using Printf: @printf

struct Record
    meta::UInt64
    data::String

    function Record(data::String)
        len = sizeof(data)
        meta = len % UInt64
        if len > 0
            # mix the first and the last byte
            @inbounds meta |= (codeunit(data,   1) % UInt64) << 56
            @inbounds meta |= (codeunit(data, len) % UInt64) << 48
        end
        return new(meta, data)
    end
end

const EMPTY_RECORD = Record("")

mutable struct Stats
    hit::Int
    miss::Int
end

struct StringCache
    maxsize::Int
    records::Vector{Record}
    stats::Stats

    function StringCache(maxsize::Int)
        @assert maxsize > 0
        return new(maxsize, [EMPTY_RECORD], Stats(0, 0))
    end
end

function Base.show(io::IO, cache::StringCache)
    stats = cache.stats
    total = stats.hit + stats.miss
    @printf(
        io,
        "StringCache: maxsize = %d, #records = %d, cache hit = %.3f (%d/%d)",
        cache.maxsize,
        length(cache.records),
        stats.hit / total,
        stats.hit,
        total,
    )
end

# LRU caching
function allocate!(cache::StringCache, p::Ptr{UInt8}, length::Int64)
    meta = length % UInt64 | UInt64(unsafe_load(p)) << 56 | UInt64(unsafe_load(p + length - 1)) << 48
    records = cache.records
    stats = cache.stats
    # unroll the first loop of the linear search for performance
    @inbounds r = records[1]
    if r.meta == meta && memcmp(p, pointer(r.data), length) == 0
        stats.hit += 1
        return r.data
    end
    n = Base.length(records)
    @inbounds for i in 2:n
        r = records[i]
        if r.meta == meta && memcmp(p, pointer(r.data), length) == 0
            stats.hit += 1
            for j in i:-1:2
                records[j] = records[j-1]
            end
            records[1] = r
            return r.data
        end
    end
    # not found
    stats.miss += 1
    string = unsafe_string(p, length)
    newrecord = Record(string)
    if n < cache.maxsize
        push!(records, EMPTY_RECORD)
        n += 1
    end
    @inbounds for j in n:-1:2
        records[j] = records[j-1]
    end
    records[1] = newrecord
    return string
end

@inline function memcmp(p1::Ptr, p2::Ptr, length::Int64)
    return ccall(:memcmp, Cint, (Ptr{Cvoid}, Ptr{Cvoid}, Csize_t), p1, p2, length)
end
