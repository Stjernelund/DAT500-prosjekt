#! /usr/bin/python3

from MRLSH import MRLSH
from MROneHot import MROneHot
import io


lsh = MRLSH()
ngrams = None
with lsh.make_runner() as runner:
    runner._input_paths = ['papers2.csv']
    runner.run()
    for _, value in lsh.parse_output(runner.cat_output()):
        ngrams = value


with open('ngramsOutput.txt', 'w+') as f:
    f.write(ngrams)

onehot = MROneHot()
with onehot.make_runner() as runner:
    runner._input_paths = ['ngramsOutput.txt']
    runner.run()
    for _, value in onehot.parse_output(runner.cat_output()):
        print(value)
