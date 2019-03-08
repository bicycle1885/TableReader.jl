using TableReader
using Test

@testset "readtsv" begin
    buffer = IOBuffer("""
    col1\tcol2\tcol3
    1\t2\t3
    4\t5\t6
    """)
    df = readtsv(buffer)
    @test eof(buffer)
    @test names(df) == [:col1, :col2, :col3]
    @test df[:col1] == [1, 4]
    @test df[:col2] == [2, 5]
    @test df[:col3] == [3, 6]
end
