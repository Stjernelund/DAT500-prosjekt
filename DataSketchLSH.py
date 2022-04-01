#! /usr/bin/python3

from datasketch import MinHash, MinHashLSH
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol


class MRDataSketchLSH(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper)]

    def mapper(self, _, line):
        key, line = line.split("\t")
        m = MinHash(num_perm=128)
        for d in set(line):
            m.update(d.encode("utf8"))
        yield key, str(m)


if __name__ == "__main__":
    MRDataSketchLSH.run()
