RESULTS = \
	results/diamonds.tablereader.csv \
	results/diamonds.csv.csv \
	results/diamonds.csvfiles.csv \
	results/diamonds.pandas.csv \
	results/flights14.tablereader.csv \
	results/flights14.csv.csv \
	results/flights14.csvfiles.csv \
	results/flights14.pandas.csv \
	results/winemag-data-130k-v2.tablereader.csv \
	results/winemag-data-130k-v2.csv.csv \
	results/winemag-data-130k-v2.csvfiles.csv \
	results/winemag-data-130k-v2.pandas.csv \
	results/parking-citations.tablereader.csv \
	results/parking-citations.csv.csv \
	results/parking-citations.pandas.csv \
	results/tmp0u3qt3mu-notab.tablereader.csv \
	results/tmp0u3qt3mu-notab.csv.csv \
	results/tmp0u3qt3mu-notab.csvfiles.csv \
	results/tmp0u3qt3mu-notab.pandas.csv \
	results/dge.tablereader.csv \
	results/dge.pandas.csv

.PHONY: benchmark
benchmark: $(RESULTS)

.PHONY: clean
clean:
	rm -f $(RESULTS)


# Default parameters

results/%.tablereader.csv: data/%.csv
	julia benchmark.jl $< >$@

results/%.csv.csv: data/%.csv
	julia benchmark.csv.jl $< >$@

results/%.csvfiles.csv: data/%.csv
	julia benchmark.csvfiles.jl $< >$@

results/%.pandas.csv: data/%.csv
	python benchmark.py $< >$@


# Special parameters

results/parking-citations.tablereader.csv: data/parking-citations.csv
	julia benchmark.jl $< "(chunkbits = 0,)" >$@

results/parking-citations.csv.csv: data/parking-citations.csv
	julia benchmark.csv.jl $< "(types = Dict(1 => String, 8 => Union{String,Missing}),)" >$@

results/parking-citations.csvfiles.csv: data/parking-citations.csv
	julia benchmark.csvfiles.jl $< "(colparsers = Dict(1 => String),)" >$@

results/parking-citations.pandas.csv: data/parking-citations.csv
	python benchmark.py $< '{"low_memory": False}' >$@

results/tmp0u3qt3mu-notab.csvfiles.csv: data/tmp0u3qt3mu-notab.csv
	julia benchmark.csvfiles.jl $< '(colparsers = Dict(24 => String),)' >$@

results/tmp0u3qt3mu-notab.pandas.csv: data/tmp0u3qt3mu-notab.csv
	python benchmark.py $< '{"low_memory": False}' >$@

results/dge.tablereader.csv: data/dge.txt
	julia benchmark.jl $< "(delim = '\t',)" >$@

results/dge.csv.csv: data/dge.txt
	julia benchmark.csv.jl $< "(delim = '\t',)" >$@

results/dge.pandas.csv: data/dge.txt
	python benchmark.py $< '{"sep": "\t"}' >$@


# Data sets

data/parking-citations.csv:
	unzip -p data/los-angeles-parking-citations.zip parking-citations.csv >$@

data/tmp0u3qt3mu-notab.csv:
	perl -ple 's/\t/ /g' data/tmp0u3qt3mu.csv >$@

data/winemag-data-130k-v2.csv:
	unzip -p data/wine-reviews.zip winemag-data-130k-v2.csv >$@

data/dge.txt:
	gzip -kd data/dge.txt.gz
