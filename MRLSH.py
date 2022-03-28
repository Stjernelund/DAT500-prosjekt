#! /usr/bin/python3

import numpy as np
from mrjob.job import MRJob


class MRLSH(MRJob):
    def reducer(self, key, values):
        yield None, sum(1 for _ in values)


if __name__ == '__main__':
    MRLSH.run()
