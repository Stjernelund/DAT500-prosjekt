#! /usr/bin/python3

from datasketch import MinHash, MinHashLSH
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob import protocol
import ast
import json


class MRDataSketchLSH(MRJob):
    mrjobs = []
    num_prem = 128

    def init(self, threshold):
        self.threshold = threshold

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

    def find_similar(self, lsh):
        similar = {}
        for key, job in self.mrjobs:
            found = lsh.query(job)
            found.remove(key)
            if found:
                similar[key] = found
        with open("similar.txt", "w+") as output:
            for key, line in similar:
                output.write(f"{key}\t{line}\n")


if __name__ == "__main__":
    MRDataSketchLSH.run()
