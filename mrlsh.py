#!/usr/bin/env python3
# -*-coding:utf-8 -*

from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
from datasketch import MinHash, MinHashLSH, LeanMinHash


class DataSketchLSH(MRJob):
    num_prem = 128
    threshold = 1.0

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
            )
        ]

    def mapper_init(self):
        self.lsh = MinHashLSH(threshold=self.threshold, num_perm=self.num_prem)

    def mapper(self, _, line):
        pid, line = line.split("\t")
        pid = pid.strip('\\"')
        m = MinHash(num_perm=self.num_prem)
        line = ast.literal_eval(line)
        for d in line:
            m.update(str(d).encode("utf8"))
        m = LeanMinHash(m)
        self.lsh.insert(pid, m)
        similars = self.lsh.query(m)
        similars.remove(pid)
        if similars:
            yield pid, similars


if __name__ == "__main__":
    DataSketchLSH.run()
