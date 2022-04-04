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
shutil.rmtree('outputB')
preprocesser = MRPreProcess()
with preprocesser.make_runner() as runner:
    runner._input_paths = ["papers.csv"]
    runner._output_dir = "outputB"
    runner.run()
"""

preprostime = time.time()
print(preprostime - start)

shutil.rmtree("outputB2")
datasketch = MRDataSketchLSH()
with datasketch.make_runner() as runner:
    runner._input_paths = ["outputB/part-*"]
    runner._output_dir = "outputB2"
    runner.run()

minhashtime = time.time()
print(minhashtime - preprostime)
lsh = datasketch.make_LSH()

print(time.time() - minhashtime)
# item = datasketch.get_item(1)
print(lsh.query(datasketch.first))
