using TableReader
using Test

@testset "readtsv" begin
    @testset "simple" begin
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

    @testset "trimming" begin
        # trimming space
        buffer = IOBuffer("""
        col1\tcol2\tcol3
        1   \t   2\t   3
           4\t   5\t   6
          7 \t 8  \t 9  
        """)
        df = readtsv(buffer)
        @test df[:col1] == [1, 4, 7]
        @test df[:col2] == [2, 5, 8]
        @test df[:col3] == [3, 6, 9]

        buffer = IOBuffer("""
         col1  \t col2 \t col3  
         foo   \t  b  \t baz
        """)
        df = readtsv(buffer)
        @test df[:col1] == ["foo"]
        @test df[:col2] == ["b"]
        @test df[:col3] == ["baz"]
    end
end
