#! /usr/bin/python3

from datasketch import MinHash, MinHashLSH
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol
import ast


class MRDataSketchLSH(MRJob):
    mrjobs = []

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        key, line = line.split("\t")
        m = MinHash(num_perm=128)
        for d in ast.literal_eval(line):
            m.update(str(d).encode("utf8"))
        self.mrjobs.append((key, m))

    def reducer(self, _, values):
        yield None, list(values)

    def make_LSH(self):
        lsh = MinHashLSH(threshold=0.99, num_perm=128)
        for key, m in self.mrjobs:
            lsh.insert(key, m)
        return lsh

    def get_item(self, index):
        return self.mrjobs[index][1]


if __name__ == "__main__":
    MRDataSketchLSH.run()
