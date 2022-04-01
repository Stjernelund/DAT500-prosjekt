#! /usr/bin/python3

from datasketch import MinHash, MinHashLSH
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol


class MRDataSketchLSH(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        key, line = line.split("\t")
        m = MinHash(num_perm=128)
        for d in set(line):
            m.update(d.encode("utf8"))
        yield key, m.hashvalues

    def reducer(self, key, m):
        lsh = MinHashLSH(threshold=0.5)
        for hash in m:
            lsh.insert(key, hash)
        yield list(key), lsh


if __name__ == "__main__":
    MRDataSketchLSH.run()
