# String Cache
# ------------

using Printf: @printf

struct Record
    meta::UInt64
    data::String

    function Record(data::String)
        len = length(data) % UInt64
        meta = len
        if len > 0
            # mix the first and the last byte
            meta |= (codeunit(data, 1) % UInt64) << 56
            meta |= (codeunit(data, len) % UInt64) << 48
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
        return new(maxsize, Record[], Stats(0, 0))
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
function allocate!(cache::StringCache, p::Ptr{UInt8}, length::UInt64)
    meta = length | ((unsafe_load(p) % UInt64) << 56) | ((unsafe_load(p + length - 1) % UInt64) << 48)
    records = cache.records
    n = Base.length(records)
    stats = cache.stats
    @inbounds for i in 1:n
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

@inline function memcmp(p1::Ptr, p2::Ptr, length::UInt64)
    return ccall(:memcmp, Cint, (Ptr{Cvoid}, Ptr{Cvoid}, Csize_t), p1, p2, length)
end
