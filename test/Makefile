.PHONY: all
all: test.csv.gz test.csv.xz test.csv.zst test.tsv test.tsv.gz test.tsv.xz test.tsv.zst

.PHONY: clean
clean:
	rm -f test.tsv *.gz *.xz *.zst

%.tsv: %.csv
	perl -ple 's/,/\t/g' $< >$@

%.gz: %
	gzip -k $<

%.xz: %
	xz -k $<

%.zst: %
	zstd -k $<
