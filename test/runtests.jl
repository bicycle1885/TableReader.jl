using TableReader
using Dates
using Test

@testset "readtsv" begin
    @testset "simple" begin
        # no data
        buffer = IOBuffer("""
        col1
        """)
        df = readtsv(buffer)
        @test eof(buffer)
        @test names(df) == [:col1]
        @test size(df) == (0, 1)

        buffer = IOBuffer("""
        col1\tcol2\tcol3
        """)
        df = readtsv(buffer)
        @test eof(buffer)
        @test names(df) == [:col1, :col2, :col3]
        @test size(df) == (0, 3)

        # integers
        buffer = IOBuffer("""
        col1\tcol2\tcol3
        1\t23\t456
        -10\t-99\t0
        """)
        df = readtsv(buffer)
        @test eof(buffer)
        @test names(df) == [:col1, :col2, :col3]
        @test df[:col1] == [1, -10]
        @test df[:col2] == [23, -99]
        @test df[:col3] == [456, 0]

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

        # bools
        buffer = IOBuffer("""
        col1\tcol2
        true\tfalse
        True\tFalse
        TrUe\tFaLsE
        TRUE\tFALSE
        t\tf
        T\tF
        """)
        df = readtsv(buffer)
        @test eof(buffer)
        @test names(df) == [:col1, :col2]
        @test df[:col1] == [true, true, true, true, true, true]
        @test df[:col2] == [false, false, false, false, false, false]

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

        # dates
        buffer = IOBuffer("""
        col1\tcol2\tcol3
        2015-12-21\t2019-01-01\t1999-09-11
        """)
        df = readtsv(buffer)
        @test eof(buffer)
        @test names(df) == [:col1, :col2, :col3]
        @test df[:col1] == [Date(2015, 12, 21)]
        @test df[:col2] == [Date(2019, 1, 1)]
        @test df[:col3] == [Date(1999, 9, 11)]

        # datetimes
        buffer = IOBuffer("""
        col1\tcol2\tcol3
        2015-12-21T00:00:00\t2015-12-21T11:22:33\t2015-12-21T11:22:33.444
        """)
        df = readtsv(buffer)
        @test eof(buffer)
        @test names(df) == [:col1, :col2, :col3]
        @test df[:col1] == [DateTime(2015, 12, 21, 0, 0, 0)]
        @test df[:col2] == [DateTime(2015, 12, 21, 11, 22, 33)]
        @test df[:col3] == [DateTime(2015, 12, 21, 11, 22, 33, 444)]

        # datetimes (delimited by space)
        buffer = IOBuffer("""
        col1\tcol2\tcol3
        2015-12-21 00:00:00\t2015-12-21 11:22:33\t2015-12-21 11:22:33.444
        """)
        df = readtsv(buffer)
        @test eof(buffer)
        @test names(df) == [:col1, :col2, :col3]
        @test df[:col1] == [DateTime(2015, 12, 21, 0, 0, 0)]
        @test df[:col2] == [DateTime(2015, 12, 21, 11, 22, 33)]
        @test df[:col3] == [DateTime(2015, 12, 21, 11, 22, 33, 444)]
    end

    @testset "tricky float" begin
        # NaN
        buffer = IOBuffer("""
        col1\tcol2\tcol3\tcol4
        nan\tNan\tNaN\tNAN
        -nan\t+Nan\t-NaN\t+NAN
        """)
        df = readtsv(buffer)
        @test all(isnan.(df[:col1]))
        @test all(isnan.(df[:col2]))
        @test all(isnan.(df[:col3]))
        @test all(isnan.(df[:col4]))

        # Inf
        buffer = IOBuffer("""
        col1\tcol2\tcol3\tcol4
        inf\tInf\tInF\tINF
        -inf\t+Inf\t-InF\t+INF
        infinity\tInfinity\tInFiNiTy\tINFINITY
        -infinity\t+Infinity\t-InFiNiTy\t+INFINITY
        """)
        df = readtsv(buffer)
        @test all(isinf.(df[:col1]))
        @test all(isinf.(df[:col2]))
        @test all(isinf.(df[:col3]))
        @test all(isinf.(df[:col4]))
    end

    @testset "quotation" begin
        data = """
        "col1"\t"col2"\t"col3"
        "1"\t"23"\t"456"
        "-10"\t"-99"\t"0"
        """
        df = readtsv(IOBuffer(data))
        @test df[:col1] == [1, -10]
        @test df[:col2] == [23, -99]
        @test df[:col3] == [456, 0]

        # floats
        buffer = IOBuffer("""
        "col1"\t"col2"\t"col3"
        "1.0"\t"1.1"\t"12.34"
        "-1.2"\t"0.0"\t"-9."
        ".000"\t".123"\t"100.000"
        "1e3"\t"1.E+123"\t"-8.2e-00"
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
        "a"\t"b"\t"c"
        "foo"\t"bar"\t"baz"
        """)
        df = readtsv(buffer)
        @test eof(buffer)
        @test names(df) == [:col1, :col2, :col3]
        @test df[:col1] == ["a", "foo"]
        @test df[:col2] == ["b", "bar"]
        @test df[:col3] == ["c", "baz"]

        buffer = IOBuffer("""
        col1\tcol2\tcol3
        foo\tbar\tbaz
        foo\tbar\tbaz
        """)
        df = readtsv(buffer)
        @test df[:col1] == ["foo", "foo"]
        @test df[:col2] == ["bar", "bar"]
        @test df[:col3] == ["baz", "baz"]

        # quotation marks in a quoted string
        buffer = IOBuffer("""
        col1\tcol2
        " ""OK"" "\t"\""OK"\""
        """)
        df = readtsv(buffer)
        @test df[:col1] == [" \"OK\" "]
        @test df[:col2] == ["\"OK\""]
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

        # NA
        data = """
        col1\tcol2
        1\t2
        NA\tNA
        "NA"\t"NA"
        """
        df = readtsv(IOBuffer(data))
        @test df[1,:col1] == 1
        @test ismissing(df[2,:col1])
        @test ismissing(df[3,:col1])
        @test df[1,:col2] == 2
        @test ismissing(df[2,:col2])
        @test ismissing(df[3,:col2])
    end

    @testset "NA" begin
        # NA is case-sensitive.
        data = """
        col1\tcol2
        na\tNa
        NA\tNA
        "Na"\t"nA"
        "NA"\t"NA"
        """
        df = readtsv(IOBuffer(data))
        @test df[1,:col1] == "na"
        @test ismissing(df[2,:col1])
        @test df[3,:col1] == "Na"
        @test ismissing(df[4,:col1])
        @test df[1,:col2] == "Na"
        @test ismissing(df[2,:col2])
        @test df[3,:col2] == "nA"
        @test ismissing(df[4,:col2])
    end

    @testset "large integer" begin
        data = """
        col1\tcol2
        $(typemax(Int))\t$(typemin(Int))
        """
        df = readtsv(IOBuffer(data))
        @test df[:col1] == [typemax(Int)]
        @test df[:col2] == [typemin(Int)]

        # not supported
        data = """
        col1
        99999999999999999999999
        """
        @test_throws OverflowError readtsv(IOBuffer(data))
    end

    @testset "malformed data" begin
        # empty
        @test_throws TableReader.ReadError("found no column names in the header at line 1") readtsv(IOBuffer(""))

        # less columns than expected
        data = """
        col1\tcol2\tcol3
        foo\tbar
        """
        @test_throws TableReader.ReadError("unexpected number of columns at line 2") readtsv(IOBuffer(data))

        # more columns than expected
        data = """
        col1\tcol2\tcol3
        foo\tbar\tbaz\tqux\tquux
        """
        @test_throws TableReader.ReadError("unexpected number of columns at line 2") readtsv(IOBuffer(data))

        # too large floating-point number
        data = """
        col1\tcol2
        1.0\t1e500
        """
        @test_throws TableReader.ReadError("failed to parse a floating-point number") readtsv(IOBuffer(data))
    end

    @testset "UTF-8 strings" begin
        data = """
        col1\tcol2\tcol3
        甲\t乙\t丙
        👌\t😀😀😀\t🐸🐓
        """
        df = readtsv(IOBuffer(data))
        @test df[:col1] == ["甲", "👌"]
        @test df[:col2] == ["乙", "😀😀😀"]
        @test df[:col3] == ["丙", "🐸🐓"]
    end

    @testset "CR+LF" begin
        data = """
        col1\tcol2\tcol3\r
        1\t2\t3\r
        4\t5\t6 \r
        """
        df = readtsv(IOBuffer(data))
        @test df[:col1] == [1, 4]
        @test df[:col2] == [2, 5]
        @test df[:col3] == [3, 6]

        data = """
        col1\tcol2\tcol3\r
        1.0\t2.0\t3.0\r
        4.0\t5.0\t6.0 \r
        """
        df = readtsv(IOBuffer(data))
        @test df[:col1] == [1.0, 4.0]
        @test df[:col2] == [2.0, 5.0]
        @test df[:col3] == [3.0, 6.0]

        data = """
        col1\tcol2\tcol3\r
        foo\tbar\tbaz\r
        hoge\tfuga\tpiyo \r
        """
        df = readtsv(IOBuffer(data))
        @test df[:col1] == ["foo", "hoge"]
        @test df[:col2] == ["bar", "fuga"]
        @test df[:col3] == ["baz", "piyo"]
    end

    @testset "CR" begin
        data = """col1\tcol2\r123\t456\r"""
        df = readtsv(IOBuffer(data))
        @test df[:col1] == [123]
        @test df[:col2] == [456]
    end

    @testset "blank lines" begin
        # before header
        data = """


        col1\tcol2
        1\t2
        3\t4
        """
        df = readtsv(IOBuffer(data))
        @test df[:col1] == [1, 3]
        @test df[:col2] == [2, 4]

        # after header
        data = """
        col1\tcol2


        1\t2
        3\t4
        """
        df = readtsv(IOBuffer(data))
        @test df[:col1] == [1, 3]
        @test df[:col2] == [2, 4]

        # among data
        data = """
        col1\tcol2
        1\t2


        3\t4
        """
        df = readtsv(IOBuffer(data))
        @test df[:col1] == [1, 3]
        @test df[:col2] == [2, 4]

        # end of a file
        data = """
        col1\tcol2
        1\t2
        3\t4


        """
        df = readtsv(IOBuffer(data))
        @test df[:col1] == [1, 3]
        @test df[:col2] == [2, 4]
    end

    @testset "invalid argument" begin
        @test_throws ArgumentError readtsv(IOBuffer(""), chunkbits = -1)
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
        data = take!(buf)

        # with chunking
        df = readtsv(IOBuffer(data))
        @test size(df) == (m, n + 1)
        @test df[:name] == ["row$(i)" for i in 1:m]
        @test df[:col1] == 1:m
        @test df[:col2] == 1:m

        # without chunking
        df = readtsv(IOBuffer(data), chunkbits = 0)
        @test size(df) == (m, n + 1)
        @test df[:name] == ["row$(i)" for i in 1:m]
        @test df[:col1] == 1:m
        @test df[:col2] == 1:m
    end

    @testset "from file" begin
        df = readtsv(joinpath(@__DIR__, "test.tsv"))
        @test df[:col1] == [1, 2]
        @test df[:col2] == [1.0, 2.0]
        @test df[:col3] == ["one", "two"]

        df = readtsv(joinpath(@__DIR__, "test.tsv.gz"))
        @test df[:col1] == [1, 2]
        @test df[:col2] == [1.0, 2.0]
        @test df[:col3] == ["one", "two"]

        df = readtsv(joinpath(@__DIR__, "test.tsv.zst"))
        @test df[:col1] == [1, 2]
        @test df[:col2] == [1.0, 2.0]
        @test df[:col3] == ["one", "two"]

        df = readtsv(joinpath(@__DIR__, "test.tsv.xz"))
        @test df[:col1] == [1, 2]
        @test df[:col2] == [1.0, 2.0]
        @test df[:col3] == ["one", "two"]
    end
end

@testset "readcsv" begin
    @testset "simple" begin
        # integers
        buffer = IOBuffer("""
        col1,col2,col3
        1,23,456
        -10,-99,0
        """)
        df = readcsv(buffer)
        @test eof(buffer)
        @test names(df) == [:col1, :col2, :col3]
        @test df[:col1] == [1, -10]
        @test df[:col2] == [23, -99]
        @test df[:col3] == [456, 0]

        # floats
        buffer = IOBuffer("""
        col1,col2,col3
        1.0,1.1,12.34
        -1.2,0.0,-9.
        .000,.123,100.000
        1e3,1.E+123,-8.2e-00
        """)
        df = readcsv(buffer)
        @test eof(buffer)
        @test names(df) == [:col1, :col2, :col3]
        @test df[:col1] == [1.0, -1.2, 0.000, 1e3]
        @test df[:col2] == [1.1, 0.0, 0.123, 1e123]
        @test df[:col3] == [12.34, -9.0, 100.000, -8.2]

        # strings
        buffer = IOBuffer("""
        col1,col2,col3
        a,b,c
        foo,bar,baz
        """)
        df = readcsv(buffer)
        @test eof(buffer)
        @test names(df) == [:col1, :col2, :col3]
        @test df[:col1] == ["a", "foo"]
        @test df[:col2] == ["b", "bar"]
        @test df[:col3] == ["c", "baz"]
    end

    @testset "no header" begin
        data = """
        A,B,C
        a,b,c
        """
        df = readcsv(IOBuffer(data))
        @test df[:A] == ["a"]
        @test df[:B] == ["b"]
        @test df[:C] == ["c"]

        df = readcsv(IOBuffer(data), hasheader = false)
        @test df[:X1] == ["A", "a"]
        @test df[:X2] == ["B", "b"]
        @test df[:X3] == ["C", "c"]

        df = readcsv(IOBuffer(data), colnames = [:C1, :C2, :C3])
        @test df[:C1] == ["A", "a"]
        @test df[:C2] == ["B", "b"]
        @test df[:C3] == ["C", "c"]

        df = readcsv(IOBuffer(data), colnames = [:C1, :C2, :C3], hasheader = true)
        @test df[:C1] == ["a"]
        @test df[:C2] == ["b"]
        @test df[:C3] == ["c"]
    end

    @testset "quotation" begin
        data = """
        "col1","col2"
        "hi, there",","
        "1,2,3,4", ",,,"
        """
        df = readcsv(IOBuffer(data))
        @test df[:col1] == ["hi, there", "1,2,3,4"]
        @test df[:col2] == [",", ",,,"]
    end

    @testset "missing" begin
        data = """
        "col1","col2"
        "",""
        "1","2"
        """
        df = readcsv(IOBuffer(data))
        @test ismissing(df[1,:col1])
        @test df[2,:col1] == 1
        @test ismissing(df[1,:col2])
        @test df[2,:col2] == 2
    end

    @testset "multiline field" begin
        data = """
        col1,col2
        "oh,
        there
        there","
        multi
        line
        field
        "
        """
        df = readcsv(IOBuffer(data))
        @test df[:col1] == ["oh,\nthere\nthere"]
        @test df[:col2] == ["\nmulti\nline\nfield\n"]
    end

    @testset "leading-zero string" begin
        data = """
        col1,col2
        0000,0
        0001,1
        0002,2
        0123,123
        """
        df = readcsv(IOBuffer(data))  # lzstring = true by default
        @test df[:col1] == ["0000", "0001", "0002", "0123"]
        @test df[:col2] == [0, 1, 2, 123]

        df = readcsv(IOBuffer(data), lzstring = true)
        @test df[:col1] == ["0000", "0001", "0002", "0123"]
        @test df[:col2] == [0, 1, 2, 123]

        df = readcsv(IOBuffer(data), lzstring = false)
        @test df[:col1] == [0, 1, 2, 123]
        @test df[:col2] == [0, 1, 2, 123]

        data = """
        col1
        0.3
        00.4
        """
        df = readcsv(IOBuffer(data))  # lzstring = true by default
        @test df[:col1] == ["0.3", "00.4"]

        df = readcsv(IOBuffer(data), lzstring = true)
        @test df[:col1] == ["0.3", "00.4"]

        df = readcsv(IOBuffer(data), lzstring = false)
        @test df[:col1] == [0.3, 0.4]
    end

    @testset "EOF without newline" begin
        data = """
        col1,col2
        1,2"""
        df = readcsv(IOBuffer(data))
        @test df[:col1] == [1]
        @test df[:col2] == [2]
    end

    @testset "unnamed column" begin
        data = """
        col1,,col3,
        1,foo,3,bar
        """
        df = readcsv(IOBuffer(data))
        @test df[:col1] == [1]
        @test df[:UNNAMED_2] == ["foo"]
        @test df[:col3] == [3]
        @test df[:UNNAMED_4] == ["bar"]
    end

    @testset "implicit column" begin
        data = """
        col1,col2,col3
        foo,1,2,3
        """
        df = readcsv(IOBuffer(data))
        @test df[:UNNAMED_0] == ["foo"]
        @test df[:col1] == [1]
        @test df[:col2] == [2]
        @test df[:col3] == [3]
    end

    @testset "skip lines" begin
        data = """
        foobarbaz
        hogehogehoge
        col1,col2,col3
        1,2,3
        """
        df = readcsv(IOBuffer(data), skip = 2)
        @test df[:col1] == [1]
        @test df[:col2] == [2]
        @test df[:col3] == [3]
    end

    @testset "comment" begin
        data = """
        # comment before header
        col1,col2
        # comment after header
        1,2
        # comment among data
        3,4
        # comment after data
        """
        df = readcsv(IOBuffer(data), comment = "#")
        @test df[:col1] == [1, 3]
        @test df[:col2] == [2, 4]

        data = """
        ## comment before header
        col1,col2
        ## comment after header
        foo,bar
        ## comment among data
        #foo,#bar
        ## comment after data
        """
        df = readcsv(IOBuffer(data), comment = "##")
        @test df[:col1] == ["foo", "#foo"]
        @test df[:col2] == ["bar", "#bar"]

        @test_throws ArgumentError("comment cannot contain newline characters") readcsv(IOBuffer(data), comment = "\n")
        @test_throws ArgumentError("comment cannot contain newline characters") readcsv(IOBuffer(data), comment = "\r")
    end

    @testset "malformed data" begin
        # empty
        @test_throws TableReader.ReadError("found no column names in the header at line 1") readcsv(IOBuffer(""))

        # tab
        data = """
        col1,col2
        foo,ba\tr
        """
        @test_throws TableReader.ReadError("invalid file format at line 2, column 2 (found '\\t')") readcsv(IOBuffer(data))
    end

    @testset "from file" begin
        df = readcsv(joinpath(@__DIR__, "test.csv"))
        @test df[:col1] == [1, 2]
        @test df[:col2] == [1.0, 2.0]
        @test df[:col3] == ["one", "two"]

        df = readcsv(joinpath(@__DIR__, "test.csv.gz"))
        @test df[:col1] == [1, 2]
        @test df[:col2] == [1.0, 2.0]
        @test df[:col3] == ["one", "two"]

        df = readcsv(joinpath(@__DIR__, "test.csv.zst"))
        @test df[:col1] == [1, 2]
        @test df[:col2] == [1.0, 2.0]
        @test df[:col3] == ["one", "two"]

        df = readcsv(joinpath(@__DIR__, "test.csv.xz"))
        @test df[:col1] == [1, 2]
        @test df[:col2] == [1.0, 2.0]
        @test df[:col3] == ["one", "two"]
    end

    @testset "from command" begin
        if Sys.which("echo") === nothing
            @info "skip tests: echo command is not found"
        else
            df = readcsv(`echo $("col1,col2\n1,2")`)
            @test df[:col1] == [1]
            @test df[:col2] == [2]
        end

        if Sys.which("cat") === nothing || Sys.which("gzip") === nothing
            @info "skip tests: cat/gzip commands are not found"
        else
            testfile = joinpath(@__DIR__, "test.csv")
            df = readcsv(pipeline(`cat $(testfile)`, `gzip`))
            @test df[:col1] == [1, 2]
            @test df[:col2] == [1.0, 2.0]
            @test df[:col3] == ["one", "two"]
        end
    end

    @testset "switching from strings to numbers" begin
        buf = IOBuffer()
        println(buf, "col1,col2")
        for i in 1:1_000_000
            if i % 1024 == 0
                println(buf, "foo,")  # missing
            else
                println(buf, "foo,bar")
            end
        end
        for i in 1:1_000_000
            println(buf, "1,2")
        end
        for i in 1:1_000_000
            println(buf, "1.1,2.2")
        end
        seekstart(buf)
        df = readcsv(buf)
        @test df[:col1] isa Vector{String}
        @test df[:col2] isa Vector{Union{String,Missing}}
    end

    @testset "repeated variable names" begin
        data = """
            col1,col1
            1,2
            """
            df = readcsv(IOBuffer(data))
            @test df[:col1_1] == [2]
    end
end

@testset "readdlm" begin
    @testset "custom parser parameters" begin
        # delimtied by |
        data = """
        col1|col2|col3
        1|2|3
        """
        df = readdlm(IOBuffer(data), delim = '|')
        @test df[:col1] == [1]
        @test df[:col2] == [2]
        @test df[:col3] == [3]

        # quoted by `
        data = """
        col1,col2,col3
        `foo`,`bar`,`baz`
        """
        df = readdlm(IOBuffer(data), delim = ',', quot = '`')
        @test df[:col1] == ["foo"]
        @test df[:col2] == ["bar"]
        @test df[:col3] == ["baz"]

        # invalid chunkbits
        @test_throws ArgumentError readdlm(IOBuffer(""), delim = ',', chunkbits = -1)
        @test_throws ArgumentError readdlm(IOBuffer(""), delim = ',', chunkbits = 100)
    end

    @testset "delimiter detection" begin
        # comma delimited
        data = """
        col1,col2,col3
        1,2,3
        """
        df = readdlm(IOBuffer(data))
        @test df[:col1] == [1]
        @test df[:col2] == [2]
        @test df[:col3] == [3]

        # tab delimited
        data = """
        col1\tcol2\tcol3
        1\t2\t3
        """
        df = readdlm(IOBuffer(data))
        @test df[:col1] == [1]
        @test df[:col2] == [2]
        @test df[:col3] == [3]

        # pipe delimited
        data = """
        col1|col2|col3
        1|2|3
        """
        df = readdlm(IOBuffer(data))
        @test df[:col1] == [1]
        @test df[:col2] == [2]
        @test df[:col3] == [3]
    end
end

@testset "colnames" begin
    @testset "normalize" begin
        # test unusual or unsafe (for dataframe column names) strings
        buffer = IOBuffer("""
        col.1,col|2,col\$3, col4, do, α,_col7
        1,2,3,4,5,6,7
        """)
        df = readcsv(buffer,normalizenames=true)
        @test names(df) == [:col_1, :col_2, :col_3,:col4,:_do,:α,:_col7]
    end
    @testset "don't normalize" begin
        buffer = IOBuffer("""
        col.1,col|2,col\$3, col4, do, α,_col7
        1,2,3,4,5,6,7
        """)
        df = readcsv(buffer,normalizenames=false)
        @test names(df) == [Symbol("col.1"), Symbol("col|2"), Symbol("col\$3"), :col4, :do, :α, :_col7]
    end
end

@testset "download" begin
    if Sys.which("curl") === nothing
        @info "Skipped file downloading tests"
    else
        df = readcsv("https://raw.githubusercontent.com/bicycle1885/TableReader.jl/master/test/test.csv")
        @test df[:col1] == [1, 2]
        @test df[:col2] == [1.0, 2.0]
        @test df[:col3] == ["one", "two"]
    end
end


# Field-level test cases
# ----------------------

function testfield(s::String, expected)
    data = """
    col1,col2,col3
    $(s),$(s),$(s)
      $(s)  ,  $(s)  ,  $(s)  
    """
    df = readcsv(IOBuffer(data))
    x1, x2 = df[:col1]
    y1, y2 = df[:col2]
    z1, z2 = df[:col3]
    sametype = typeof(x1) == typeof(x2) ==
               typeof(y1) == typeof(y2) ==
               typeof(z1) == typeof(z2) ==
               typeof(expected)
    if expected isa Float64 && isnan(expected)
        @test sametype && isnan(x1) && isnan(x2) &&
                          isnan(y1) && isnan(y2) &&
                          isnan(z1) && isnan(z2)
    elseif expected isa Missing
        @test sametype && ismissing(x1) && ismissing(x2) &&
                          ismissing(y1) && ismissing(y2) &&
                          ismissing(z1) && ismissing(z2)
    else
        @test sametype && x1 == x2 == y1 == y2 == z1 == z2 == expected
    end
end

@testset "checkfield" begin
    @testset "integer" begin
        testfield("0", 0)
        testfield("+0", 0)
        testfield("-0", 0)
        testfield("1", 1)
        testfield("+1", 1)
        testfield("-1", -1)
        testfield("12", 12)
        testfield("+12", 12)
        testfield("-12", -12)
        testfield("1234567890", 1234567890)
        testfield("+1234567890", 1234567890)
        testfield("-1234567890", -1234567890)
        testfield("\"1234567890\"", 1234567890)
        testfield("\"+1234567890\"", 1234567890)
        testfield("\"-1234567890\"", -1234567890)
    end

    @testset "float" begin
        testfield("0.", 0.0)
        testfield("+0.", 0.0)
        testfield("-0.", -0.0)
        testfield("1.", 1.0)
        testfield("+1.", 1.0)
        testfield("-1.", -1.0)
        testfield(".0", 0.0)
        testfield("+.0", 0.0)
        testfield("-.0", -0.0)
        testfield(".1", 0.1)
        testfield("+.1", 0.1)
        testfield("-.1", -0.1)
        testfield("0.0", 0.0)
        testfield("+0.0", 0.0)
        testfield("-0.0", -0.0)
        testfield("1.0", 1.0)
        testfield("+1.0", 1.0)
        testfield("-1.0", -1.0)
        testfield("1.1", 1.1)
        testfield("+1.1", 1.1)
        testfield("-1.1", -1.1)
        testfield("123.4567890", 123.456789)
        testfield("+123.456789", 123.456789)
        testfield("-123.456789", -123.456789)
        testfield("\"-1.23\"", -1.23)
        testfield("1e3", 1e3)
        testfield("1e+3", 1e+3)
        testfield("1e-3", 1e-3)
        testfield("1E3", 1e3)
        testfield("1E+3", 1e+3)
        testfield("1E-3", 1e-3)
        testfield("+1e3", 1e3)
        testfield("-1e3", -1e3)
        testfield("123.45e67", 123.45e67)
        testfield("123.45e+67", 123.45e67)
        testfield("123.45e-67", 123.45e-67)
        testfield("\"1.2e34\"", 1.2e34)
        testfield("inf", Inf)
        testfield("iNf", Inf)
        testfield("INF", Inf)
        testfield("infinity", Inf)
        testfield("iNfInItY", Inf)
        testfield("INFINITY", Inf)
        testfield("\"inf\"", Inf)
        testfield("nan", NaN)
        testfield("nAn", NaN)
        testfield("NAN", NaN)
        testfield("\"nan\"", NaN)
    end

    @testset "bool" begin
        testfield("t", true)
        testfield("T", true)
        testfield("true", true)
        testfield("True", true)
        testfield("TrUe", true)
        testfield("TRUE", true)
        testfield("\"t\"", true)
        testfield("\"true\"", true)
        testfield("f", false)
        testfield("F", false)
        testfield("false", false)
        testfield("False", false)
        testfield("FaLsE", false)
        testfield("FALSE", false)
        testfield("\"f\"", false)
        testfield("\"false\"", false)
    end

    @testset "missing" begin
        testfield("", missing)
        testfield("\"\"", missing)
        testfield("NA", missing)
        testfield("\"NA\"", missing)
    end

    @testset "string" begin
        testfield("!", "!")
        testfield("~", "~")
        testfield(".", ".")
        testfield("..", "..")
        testfield("+", "+")
        testfield("-+", "-+")
        testfield("i", "i")
        testfield("I", "I")
        testfield("n", "n")
        testfield("N", "N")
        testfield("+12!", "+12!")
        testfield("-12!", "-12!")
        testfield("0!", "0!")
        testfield("1!", "1!")
        testfield("0..", "0..")
        testfield("1..", "1..")
        testfield("1.2.", "1.2.")
        testfield("0123", "0123")
        testfield("1e", "1e")
        testfield("1ea", "1ea")
        testfield("1e-", "1e-")
        testfield("1e+", "1e+")
        testfield("1e-!", "1e-!")
        testfield("1e+!", "1e+!")
        testfield("123 456", "123 456")
        testfield("Inf0", "Inf0")
        testfield("Infinity0", "Infinity0")
        testfield("t0", "t0")
        testfield("f0", "f0")
        testfield("true0", "true0")
        testfield("false0", "false0")
        testfield("\"a\"", "a")
        testfield("\"\"\"\"", "\"")
        testfield("\"\"\"\"\"\"", "\"\"")
    end
end

# Deprecated feature
@testset "deprecated" begin
    @info "Testing deprecated features"

    @testset "chunksize" begin
        data = """
        col1,col2
        1,2
        """
        df = readcsv(IOBuffer(data), chunksize = 0)
        @test df[:col1] == [1]
        @test df[:col2] == [2]
    end
end
