using CSVFiles, DataFrames

filename = ARGS[1]
if length(ARGS) > 1
    params = eval(Meta.parse(ARGS[2]))
else
    params = ()
end

println("package,run,elapsed,gctime,bytes")
for i in 1:6
    GC.gc(); GC.gc()
    _, elapsed, bytes, gctime, _ = @timed DataFrame(load(filename; params...))
    println("CSVFiles.jl", ',', i, ',', elapsed, ',', gctime, ',', bytes)
end

