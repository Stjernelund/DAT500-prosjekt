#! /usr/bin/python3

from datasketch import MinHash, MinHashLSH
from mrjob.job import MRJob
from mrjob.step import MRStep
import ast


class MRDataSketchLSH(MRJob):
    mrjobs = []
    num_prem = 2

    def init(self, threshold):
        """Used to set threshold"""
        self.threshold = threshold

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
            )
        ]

    def mapper(self, _, line):
        """MinHash each paper"""
        key, line = line.split("\t")
        key = key.strip('\\"')
        m = MinHash(num_perm=self.num_prem)
        yield key, line
        """
        for d in line:
            yield key, d
            m.update(str(d).encode("utf8"))
        self.mrjobs.append((key, m))
        yield None, key
        """

    def reducer(self, _, values):
        yield None, list(values)

    def make_LSH(self):
        """Create LSH index from the MinHashes"""
        lsh = MinHashLSH(threshold=self.threshold, num_perm=self.num_prem)
        for key, m in self.mrjobs:
            lsh.insert(key, m)
        return lsh

    def find_similar(self, lsh):
        """Query each paper against the others looking for similarities"""
        similar = {}
        for key, job in self.mrjobs:
            found = lsh.query(job)
            found.remove(key)
            if found:
                similar[key] = found
        with open(
            f"output_t{int(self.threshold * 100)}/similar.txt",
            "w+",
        ) as output:
            for key, line in similar.items():
                output.write(f"{key}\t{line}\n")


if __name__ == "__main__":
    MRDataSketchLSH.run()
