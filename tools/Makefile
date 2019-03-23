RESULTS = \
	results/diamonds.tablereader.csv \
	results/diamonds.pandas.csv \
	results/flights14.tablereader.csv \
	results/flights14.pandas.csv \
	results/winemag-data-130k-v2.tablereader.csv \
	results/winemag-data-130k-v2.pandas.csv \
	results/parking-citations.tablereader.csv \
	results/parking-citations.pandas.csv \
	results/tmp0u3qt3mu-notab.tablereader.csv \
	results/tmp0u3qt3mu-notab.pandas.csv \
	results/dge.tablereader.csv \
	results/dge.pandas.csv

.PHONY: benchmark
benchmark: $(RESULTS)

results/%.tablereader.csv: data/%.csv
	julia benchmark.jl $< >$@

results/%.pandas.csv: data/%.csv
	python benchmark.py $< >$@

results/parking-citations.tablereader.csv: data/parking-citations.csv
	julia benchmark.jl $< "(chunksize = 0,)" >$@

results/dge.tablereader.csv: data/dge.txt
	julia benchmark.jl $< "(delim = '\t',)" >$@

results/parking-citations.pandas.csv: data/parking-citations.csv
	python benchmark.py $< '{"low_memory": False}' >$@

results/tmp0u3qt3mu-notab.pandas.csv: data/tmp0u3qt3mu-notab.csv
	python benchmark.py $< '{"low_memory": False}' >$@

results/dge.pandas.csv: data/dge.txt
	python benchmark.py $< '{"sep": "\t"}' >$@

data/parking-citations.csv:
	unzip -p data/los-angeles-parking-citations.zip parking-citations.csv >$@

data/tmp0u3qt3mu-notab.csv:
	perl -ple 's/\t/ /g' data/tmp0u3qt3mu.csv >$@

data/winemag-data-130k-v2.csv:
	unzip -p data/wine-reviews.zip winemag-data-130k-v2.csv >$@

data/dge.txt:
	gzip -kd data/dge.txt.gz
