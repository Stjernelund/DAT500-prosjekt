#! /usr/bin/python3

from MRLSH import MRLSH


lsh = MRLSH()
with lsh.make_runner() as runner:
    print('kjører')
    runner.run()
    for key, value in lsh.parse_output(runner.cat_output()):
        print(key, value)
