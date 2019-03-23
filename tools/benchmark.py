import gc
import sys
import time
import pandas

filename = sys.argv[1]
if len(sys.argv) > 2:
    params = eval(sys.argv[2])
else:
    params = {}

print("package,run,elapsed")
for i in range(6):
    gc.collect()
    start = time.time()
    pandas.read_csv(filename, **params)
    elapsed = time.time() - start
    print("pandas", ",", i + 1, ",", elapsed, sep="")
