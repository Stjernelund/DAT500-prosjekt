#! /usr/bin/python3

from MRLSH import MRLSH


lsh = MRLSH()
with lsh.make_runner() as runner:
    print('kjører')
    runner.run()
    print(runner.cat_output())
    _, value = lsh.parse_output(runner.cat_output())
    print(value)
