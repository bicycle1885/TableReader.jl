using TableReader

if length(ARGS) != 1
    error("a function name is expected")
end

func = ARGS[1]
if func === "readcsv"
    @time readcsv("test/test.csv")
elseif func === "readtsv"
    @time readtsv("test/test.tsv")
else
    error("invalid function name: '$(func)'")
end
