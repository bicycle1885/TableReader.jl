using TableReader

filename = ARGS[1]
if length(ARGS) > 1
    params = eval(Meta.parse(ARGS[2]))
    @assert params isa NamedTuple
else
    params = ()
end

println("package,run,elapsed,gctime,bytes")
for i in 1:6
    GC.gc(); GC.gc()
    _, elapsed, bytes, gctime, _ = @timed readcsv(filename; params...)
    println("TableReader.jl", ',', i, ',', elapsed, ',', gctime, ',', bytes)
end
