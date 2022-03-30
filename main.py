#! /usr/bin/python3

from mrjob import protocol
from MRLSH import MRLSH

lsh = MRLSH()
with lsh.make_runner() as runner:
    runner._input_paths = ['papers2.csv']
    runner.OUTPUT_PROTOCOL = protocol.JSONValueProtocol
    runner.run()
    print(lsh.cat_output())
    print(lsh)
    for _, value in lsh.parse_output(runner.cat_output()):
        print(value)