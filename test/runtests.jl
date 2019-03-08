using TableReader
using Test

@testset "readtsv" begin
    # integers
    buffer = IOBuffer("""
    col1\tcol2\tcol3
    1\t2\t3
    -4\t-5\t+6
    """)
    df = readtsv(buffer)
    @test eof(buffer)
    @test names(df) == [:col1, :col2, :col3]
    @test df[:col1] == [1, -4]
    @test df[:col2] == [2, -5]
    @test df[:col3] == [3, +6]

    # strings
    buffer = IOBuffer("""
    col1\tcol2\tcol3
    a\tb\tc
    foo\tbar\tbaz
    """)
    df = readtsv(buffer)
    @test eof(buffer)
    @test names(df) == [:col1, :col2, :col3]
    @test df[:col1] == ["a", "foo"]
    @test df[:col2] == ["b", "bar"]
    @test df[:col3] == ["c", "baz"]
end
