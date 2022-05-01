#!/usr/bin/env python3
# -*-coding:utf-8 -*

from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
from datasketch import MinHash, MinHashLSH, LeanMinHash


class DataSketchLSH(MRJob):
    num_prem = 128

    def init(self, threshold):
        """Used to set threshold"""
        self.threshold = threshold

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_minhash,
                mapper=self.mapper_lsh,
                mapper=self.mapper_similar,
            )
        ]

    def mapper_init(self):
        self.lsh = MinHashLSH(threshold=self.threshold, num_perm=self.num_prem)

    def mapper_minhash(self, _, line):
        """MinHash each paper"""
        pid, line = line.split("\t")
        pid = pid.strip('\\"')
        m = MinHash(num_perm=self.num_prem)
        line = ast.literal_eval(line)
        for d in line:
            m.update(str(d).encode("utf8"))
        yield pid, m.digest().tolist()

    def mapper_lsh(self, _, line):
        pid, line = line.split("\t")
        pid = pid.strip('\\"')
        hashval = ast.literal_eval(line)
        m = LeanMinHash(hashvalues=hashval)
        self.lsh.insert(pid, m)
        yield pid, hashval

    def mapper_similar(self, _, line):
        pid, line = line.split("\t")
        pid = pid.strip('\\"')
        hashval = ast.literal_eval(line)
        similars = self.lsh.query(hashval)
        similars.remove(pid)
        yield pid, similars


'''
    def make_minhash(self, hadoop_string):
        cat = subprocess.Popen(
            ["hdfs", "dfs", "-cat", f"{hadoop_string}/ngrams/part-00000"],
            stdout=subprocess.PIPE,
        )
        for line in cat.stdout:
            line = line.split("\t")
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
        for pid, m in self.mrjobs:
            lsh.insert(pid, m)
        return lsh.get_counts()

    def find_similar(self, lsh, hadoop_string):
        """Query each paper against the others looking for similarities"""
        similar = {}
        for pid, job in self.mrjobs:
            found = lsh.query(job)
            found.remove(pid)
            if found:
                similar[pid] = found
        with open(
            f"{hadoop_string}/output_t{int(self.threshold * 100)}/similar.txt",
            "w+",
        ) as output:
            for pid, line in similar.items():
                output.write(f"{pid}\t{line}\n")
'''

if __name__ == "__main__":
    DataSketchLSH.run()
