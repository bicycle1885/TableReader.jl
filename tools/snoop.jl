using SnoopCompile

SnoopCompile.@snoop "compiles.csv" begin
    using TableReader
    readcsv("test/test.csv")
    readtsv("test/test.tsv")
end

data = SnoopCompile.read("compiles.csv")
pc = SnoopCompile.parcel(reverse!(data[2]))
SnoopCompile.write("precompile", pc)
