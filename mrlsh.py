#!/usr/bin/env python3
# -*-coding:utf-8 -*

from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
from datasketch import MinHash, MinHashLSH, LeanMinHash


class DataSketchLSH(MRJob):
    mrjobs = []
    num_prem = 128

    def init(self, threshold):
        """Used to set threshold"""
        self.threshold = threshold

    def steps(self):
        return [
            MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer)
        ]

    def mapper(self, _, line):
        """MinHash each paper"""
        key, line = line.split("\t")
        key = key.strip('\\"')
        m = MinHash(num_perm=self.num_prem)
        line = ast.literal_eval(line)
        for d in line:
            m.update(str(d).encode("utf8"))
        lean_m = LeanMinHash(seed=m.seed, hashvalues=m.hashvalues)  # Saves memoryspace
        self.mrjobs.append((key, lean_m))
        yield None, key

    def combiner(self, _, values):
        yield None, list(values)

    def reducer(self, _, values):
        yield None, list(values)

    def make_LSH(self):
        """Create LSH index from the MinHashes"""
        lsh = MinHashLSH(threshold=self.threshold, num_perm=self.num_prem)
        for key, m in self.mrjobs:
            lsh.insert(key, m)
        return lsh, self.mrjobs

    '''
    def find_similar(self, lsh, hadoop_string):
        """Query each paper against the others looking for similarities"""
        similar = {}
        for key, job in self.mrjobs:
            found = lsh.query(job)
            found.remove(key)
            if found:
                similar[key] = found
        with open(
            f"{hadoop_string}/output_t{int(self.threshold * 100)}/similar.txt",
            "w+",
        ) as output:
            for key, line in similar.items():
                output.write(f"{key}\t{line}\n")
    '''


if __name__ == "__main__":
    MRDataSketchLSH.run()
