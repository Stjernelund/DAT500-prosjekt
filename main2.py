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
    shutil.rmtree("outputBpaper2")
except FileNotFoundError:
    pass
preprocesser = MRPreProcess()
with preprocesser.make_runner() as runner:
    runner._input_paths = ["papers2.csv"]
    runner._output_dir = "outputBpaper2"
    runner.run()
"""

preprostime = time.time()
print(f"Preprocessing: {preprostime - start} seconds.")

try:
    shutil.rmtree("outputB2")
except FileNotFoundError:
    pass

datasketch = MRDataSketchLSH()
with datasketch.make_runner() as runner:
    runner._input_paths = ["outputB/part-*"]
    runner._output_dir = "outputB2"
    runner.run()

minhashtime = time.time()
print(f"Hashing: {minhashtime - preprostime} seconds.")

lsh = datasketch.make_LSH()
lshtime = time.time()
print(f"LSH: {lshtime - minhashtime} seconds.")

print(datasketch.find_similar(lsh))
print(f"Similarity: {time.time() - lshtime} seconds.")
print(f"Total time: {time.time() - start} seconds.")
