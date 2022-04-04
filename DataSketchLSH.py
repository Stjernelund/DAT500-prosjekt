#! /usr/bin/python3

from datasketch import MinHash, MinHashLSH
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol
import ast


class MRDataSketchLSH(MRJob):
    mrjobs = []
    num_prem = None
    threshold = None

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        key, line = line.split("\t")
        m = MinHash(num_perm=self.num_prem)
        for d in ast.literal_eval(line):
            m.update(str(d).encode("utf8"))
        self.mrjobs.append((key, m))
        yield None, key

    def reducer(self, _, values):
        yield None, list(values)

    def make_LSH(self):
        lsh = MinHashLSH(threshold=self.threshold, num_perm=self.num_prem)
        for key, m in self.mrjobs:
            lsh.insert(key, m)
        return lsh

    def get(self, index):
        return self.mrjobs[index][0], self.mrjobs[index][1]

    def set_options(self, threshold, num_prem):
        self.threshold = threshold
        self.num_prem = num_prem


if __name__ == "__main__":
    MRDataSketchLSH.run()
