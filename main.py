#! /usr/bin/python3

from MROneHot import MROneHot
from mrjob import protocol
from MRLSH import MRLSH

lsh = MRLSH()
with lsh.make_runner() as runner:
    runner._input_paths = ['papers2.csv']
    runner._output_dir = 'output'
    runner.run()


onehot = MROneHot()
with onehot.make_runner() as runner:
    runner._input_paths = ['output/part-00000']
    runner._output_dir = 'output'
    runner.run()
