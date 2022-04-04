#! /usr/bin/python3

from datasketch import MinHash, MinHashLSH
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol


class MRDataSketchLSH(MRJob):
    mrjobs = []
    first = None

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        key, line = line.split("\t")
        m = MinHash(num_perm=128)
        for d in set(line):
            m.update(d.encode("utf8"))
        self.mrjobs.append((key, m))
        yield None, set(line)

    def reducer(self, _, values):
        yield None, list(values)

    def make_LSH(self):
        lsh = MinHashLSH(threshold=0.99, num_perm=128)
        i = 0
        for key, m in self.mrjobs:
            if i == 0:
                self.first = m
                i += 1
            else:
                lsh.insert(key, m)
        return lsh

    def get_item(self, index):
        return self.mrjobs[index][1]


if __name__ == "__main__":
    MRDataSketchLSH.run()
