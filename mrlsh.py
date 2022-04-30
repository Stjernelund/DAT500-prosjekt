#!/usr/bin/env python3
# -*-coding:utf-8 -*

from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
from datasketch import MinHash, MinHashLSH, LeanMinHash
import subprocess


class DataSketchLSH(MRJob):
    num_prem = 128
    mrjobs = []

    def init(self, threshold, hadoop):
        """Used to set threshold"""
        self.threshold = threshold
        self.hadoop = hadoop

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        """MinHash each paper"""
        if not self.hadoop:
            key, line = line.split("\t")
            key = key.strip('\\"')
            m = MinHash(num_perm=self.num_prem)
            line = ast.literal_eval(line)
            for d in line:
                text = "".join(d)
                m.update(text.encode("utf8"))
            lean_m = LeanMinHash(
                seed=m.seed, hashvalues=m.hashvalues
            )  # Saves memoryspace
            self.mrjobs.append((key, lean_m))
            yield None, key

    def reducer(self, _, values):
        if not self.hadoop:
            yield None, list(values)

    def make_minhash(self, hadoop_string):
        cat = subprocess.Popen(
            ["hdfs", "dfs", "-cat", f"{hadoop_string}/ngrams/part-00000"],
            stdout=subprocess.PIPE,
        )
        for line in cat.stdout:
            m = MinHash(num_perm=self.num_prem)
            line = ast.literal_eval(line)
            for d in line:
                text = "".join(d)
                m.update(text.encode("utf8"))
            lean_m = LeanMinHash(
                seed=m.seed, hashvalues=m.hashvalues
            )  # Saves memoryspace
            self.mrjobs.append(lean_m)

    def make_LSH(self):
        """Create LSH index from the MinHashes"""
        lsh = MinHashLSH(threshold=self.threshold, num_perm=self.num_prem)
        for key, m in self.mrjobs:
            lsh.insert(key, m)
        return lsh.get_counts()

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


if __name__ == "__main__":
    DataSketchLSH.run()
