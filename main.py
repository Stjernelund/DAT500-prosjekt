#! /usr/bin/python3

from MRLSH import MRLSH
from MROneHot import MROneHot


lsh = MRLSH()
ngrams = None
with lsh.make_runner() as runner:
    runner._input_paths = ['papers2.csv']
    runner.run()
    for _, value in lsh.parse_output(runner.cat_output()):
        print(value)
        ngrams = value


onehot = MROneHot()
with onehot.make_runner() as runner:
    runner._stdin = ngrams
    runner.run()
    for _, value in onehot.parse_output(runner.cat_output()):
        print(value)
