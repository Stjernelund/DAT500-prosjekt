#! /usr/bin/python3

from MROneHot import MROneHot
from mrjob import protocol
from MRLSH import MRLSH
from MRPreProcess import MRPreProcess

PreProcess = MRPreProcess()
with PreProcess.make_runner() as runner:
    runner._input_paths = ['papers2.csv']
    runner._output_dir = 'output'
    runner.run()

onehot = MROneHot()
with onehot.make_runner() as runner:
    runner._input_paths = ['output/part-*']
    runner._output_dir = 'output2'
    runner.run()

lsh = MRLSH()
with lsh.make_runner() as runner:
    runner._input_paths = ['output2/part-*']
    runner._output_dir = 'output3'
    runner.run()
