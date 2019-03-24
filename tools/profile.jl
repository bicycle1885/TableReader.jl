using TableReader

# Measure elapsed time.
filename = ARGS[1]
reader = occursin(".csv", filename) ? readcsv :
         occursin(".tsv", filename) ? readtsv :
         error("the extension of the filename must .csv or .tsv")
if length(ARGS) ≥ 2
    kwargs = eval(Meta.parse(ARGS[2]))
end
for _ in 1:3
    @time reader(filename; kwargs...)
end

# Profile.
using Profile
@profile reader(filename; kwargs...)
Profile.print(IOContext(stdout, :displaysize => (10000, 10000)))
