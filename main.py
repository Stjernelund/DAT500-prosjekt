#! /usr/bin/python3

from MRLSH import MRLSH


lsh = MRLSH()
with lsh.make_runner() as runner:
    a = runner.run()
    print(a)
