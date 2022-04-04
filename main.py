#! /usr/bin/python3

from MROneHot import MROneHot
from mrjob import protocol
from MRLSH import MRLSH
from MRPreProcess import MRPreProcess
from DataSketchLSH import MRDataSketchLSH
import shutil

"""
preprocesser = MRPreProcess()
with preprocesser.make_runner() as runner:
    runner._input_paths = ["papers.csv"]
    runner._output_dir = "output"
    runner.run()


datasketch = MRDataSketchLSH()
with datasketch.make_runner() as runner:
    runner._input_paths = ["output/part-*"]
    runner._output_dir = "output4"
    runner.run()
    for key, value in datasketch.parse_output(runner.cat_output()):
        print(key, value)
"""
shutil.rmtree("outputB2")
onehot = MROneHot()
with onehot.make_runner() as runner:
    runner._input_paths = ["outputB/part-*"]
    runner._output_dir = "output2"
    runner.run()

lsh = MRLSH()
with lsh.make_runner() as runner:
    runner._input_paths = ["output2/part-*"]
    runner._output_dir = "output3"
    runner.run()
    for _, value in lsh.parse_output(runner.cat_output()):
        print(value)
