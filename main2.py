#! /usr/bin/python3

from MROneHot import MROneHot
from mrjob import protocol
from MRLSH import MRLSH
from MRPreProcess import MRPreProcess
from DataSketchLSH import MRDataSketchLSH

preprocesser = MRPreProcess()
with preprocesser.make_runner() as runner:
    runner._input_paths = ["papers2.csv"]
    runner._output_dir = "outputB"
    runner.run()


datasketch = MRDataSketchLSH()
with datasketch.make_runner() as runner:
    runner._input_paths = ["output/part-*"]
    runner._output_dir = "outputB2"
    runner.run()
    for key, value in datasketch.parse_output(runner.cat_output()):
        print(key, value)


print(datasketch.mrjobs)
