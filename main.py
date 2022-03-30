#! /usr/bin/python3

from mrjob import protocol
from MRLSH import MRLSH

lsh = MRLSH()
with lsh.make_runner() as runner:
    runner._input_paths = 'papers2.csv'
    runner.output_dir = 'outputNgram.txt'
    runner.run()
    for _, value in lsh.parse_output(runner.cat_output()):
        print(value)

