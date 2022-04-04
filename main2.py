#! /usr/bin/python3

from MROneHot import MROneHot
from mrjob import protocol
from MRLSH import MRLSH
from MRPreProcess import MRPreProcess
from DataSketchLSH import MRDataSketchLSH

preprocesser = MRPreProcess()
with preprocesser.make_runner() as runner:
    runner._input_paths = ["papers.csv"]
    runner._output_dir = "outputB"
    runner.run()

print("lager minhash")

datasketch = MRDataSketchLSH()
with datasketch.make_runner() as runner:
    runner._input_paths = ["outputB/part-*"]
    runner._output_dir = "outputB2"
    runner.run()

print("lager lsh")
lsh = datasketch.make_LSH()

item = datasketch(datasketch.get_item(1))
print(item)
print(lsh.query(item))
