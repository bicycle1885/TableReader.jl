using TableReader
using Test

@testset "readtsv" begin
    @testset "simple" begin
        # empty
        buffer = IOBuffer("")
        df = readtsv(buffer)
        @test isempty(names(df))

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

        # floats
        buffer = IOBuffer("""
        col1\tcol2\tcol3
        1.0\t1.1\t12.34
        -1.2\t0.0\t-9.
        .000\t.123\t100.000
        1e3\t1.E+123\t-8.2e-00
        """)
        df = readtsv(buffer)
        @test eof(buffer)
        @test names(df) == [:col1, :col2, :col3]
        @test df[:col1] == [1.0, -1.2, 0.000, 1e3]
        @test df[:col2] == [1.1, 0.0, 0.123, 1e123]
        @test df[:col3] == [12.34, -9.0, 100.000, -8.2]

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
        data = """
        col1\tcol2\tcol3
        1   \t   2\t   3
           4\t   5\t   6
          7 \t 8  \t 9  
        """
        df = readtsv(IOBuffer(data))
        @test df[:col1] == [1, 4, 7]
        @test df[:col2] == [2, 5, 8]
        @test df[:col3] == [3, 6, 9]

        df = readtsv(IOBuffer(data); trim = true)
        @test df[:col1] == [1, 4, 7]
        @test df[:col2] == [2, 5, 8]
        @test df[:col3] == [3, 6, 9]

        df = readtsv(IOBuffer(data); trim = false)
        @test df[:col1] == ["1   ", "   4", "  7 "]
        @test df[:col2] == ["   2", "   5", " 8  "]
        @test df[:col3] == ["   3", "   6", " 9  "]

        data = """
         col1  \t col2 \t col3  
         foo   \t  b  \t baz
        """
        df = readtsv(IOBuffer(data); trim = true)
        @test df[:col1] == ["foo"]
        @test df[:col2] == ["b"]
        @test df[:col3] == ["baz"]

        df = readtsv(IOBuffer(data); trim = false)
        @test df[Symbol(" col1  ")] == [" foo   "]
        @test df[Symbol(" col2 ")] == ["  b  "]
        @test df[Symbol(" col3  ")] == [" baz"]
    end

    @testset "missing" begin
        data = """
        col1\tcol2\tcol3
        1\t2\t3
        \t5\t
        """
        df = readtsv(IOBuffer(data))
        @test df[:col1] isa Vector{Union{Int,Missing}}
        @test df[:col2] isa Vector{Int}
        @test df[:col3] isa Vector{Union{Int,Missing}}
        @test df[1,:col1] == 1
        @test ismissing(df[2,:col1])
        @test df[1,:col2] == 2
        @test df[2,:col2] == 5
        @test df[1,:col3] == 3
        @test ismissing(df[2,:col3])

        data = """
        col1\tcol2\tcol3
        1.0\t2.2\t-9.8
        \t10\t
        """
        df = readtsv(IOBuffer(data))
        @test df[:col1] isa Vector{Union{Float64,Missing}}
        @test df[:col2] isa Vector{Float64}
        @test df[:col3] isa Vector{Union{Float64,Missing}}
        @test df[1,:col1] == 1.0
        @test ismissing(df[2,:col1])
        @test df[1,:col2] == 2.2
        @test df[2,:col2] == 10.0
        @test df[1,:col3] == -9.8
        @test ismissing(df[2,:col3])

        data = """
        col1\tcol2\tcol3
        foo\t\tbar
        baz\tqux\t
        """
        df = readtsv(IOBuffer(data))
        @test df[:col1] isa Vector{String}
        @test df[:col2] isa Vector{Union{String,Missing}}
        @test df[:col3] isa Vector{Union{String,Missing}}
        @test df[1,:col1] == "foo"
        @test df[2,:col1] == "baz"
        @test ismissing(df[1,:col2])
        @test df[2,:col2] == "qux"
        @test df[1,:col3] == "bar"
        @test ismissing(df[2,:col3])
    end

    @testset "large data" begin
        buf = IOBuffer()
        m = 10000
        n = 2000
        println(buf, "name", '\t', join(("col$(j)" for j in 1:n), '\t'))
        for i in 1:m
            print(buf, "row$(i)")
            for j in 1:n
                print(buf, '\t', i)
            end
            println(buf)
        end
        df = readtsv(IOBuffer(take!(buf)))
        @test size(df) == (m, n + 1)
        @test df[:name] == ["row$(i)" for i in 1:m]
        @test df[:col1] == 1:m
        @test df[:col2] == 1:m
    end
end
