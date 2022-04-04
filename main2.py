#! /usr/bin/python3

from MROneHot import MROneHot
from mrjob import protocol
from MRLSH import MRLSH
from MRPreProcess import MRPreProcess
from DataSketchLSH import MRDataSketchLSH
import time

start = time.time()

preprocesser = MRPreProcess()
with preprocesser.make_runner() as runner:
    runner._input_paths = ["papers2.csv"]
    runner._output_dir = "outputB"
    runner.run()

preprostime = time.time()
print(preprostime - start)

datasketch = MRDataSketchLSH()
with datasketch.make_runner() as runner:
    runner._input_paths = ["outputB/part-*"]
    runner._output_dir = "outputB2"
    runner.run()

minhashtime = time.time()
print(minhashtime - preprostime)
lsh = datasketch.make_LSH()

print(time.time() - minhashtime)
item = datasketch.get_item(1)
print(item)
print(lsh.query(item))
