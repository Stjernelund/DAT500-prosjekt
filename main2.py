#! /usr/bin/python3

from MROneHot import MROneHot
from mrjob import protocol
from MRLSH import MRLSH
from MRPreProcess import MRPreProcess
from DataSketchLSH import MRDataSketchLSH
import time
import shutil

start = time.time()

"""
try:
    shutil.rmtree("outputB")
except FileNotFoundError:
    pass
preprocesser = MRPreProcess()
with preprocesser.make_runner() as runner:
    runner._input_paths = ["papers.csv"]
    runner._output_dir = "outputB"
    runner.run()
"""

preprostime = time.time()
print(preprostime - start)

try:
    shutil.rmtree("outputB2")
except FileNotFoundError:
    pass

datasketch = MRDataSketchLSH()
datasketch.set_options(0.5, 32)

with datasketch.make_runner() as runner:
    runner._input_paths = ["outputB/part-*"]
    runner._output_dir = "outputB2"
    runner.run()

minhashtime = time.time()
print(minhashtime - preprostime)
lsh = datasketch.make_LSH()

print(time.time() - minhashtime)
key, item = datasketch.get(-1)
print(key, lsh.query(item))
