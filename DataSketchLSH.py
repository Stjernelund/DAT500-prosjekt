#! /usr/bin/python3

from datasketch import MinHash, MinHashLSH
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol


class MRDataSketchLSH(MRJob):
    mrjobs = []

    def steps(self):
        return [MRStep(mapper=self.mapper)]

    def mapper(self, _, line):
        key, line = line.split("\t")
        m = MinHash(num_perm=128)
        for d in set(line):
            m.update(d.encode("utf8"))
        self.mrjobs.append((key, m))
        yield key, None

    def make_LSH(self):
        lsh = MinHashLSH(threshold=0.5)
        for key, m in self.mrjobs:
            lsh.insert(key, m)
        return lsh


if __name__ == "__main__":
    MRDataSketchLSH.run()
