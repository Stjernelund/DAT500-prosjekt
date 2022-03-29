#! /usr/bin/python3

from MRLSH import MRLSH


lsh = MRLSH()
with lsh.make_runner() as runner:
    print('kjører')
    runner.run()
    res = lsh.parse_output(runner.cat_output())
    print(res)

    print('ok')
    for i in runner.cat_output():
        print(i)
