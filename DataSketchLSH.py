#! /usr/bin/python3

from datasketch import MinHash, MinHashLSH
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRDataSketchLSH(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper)]

    def mapper(self, key, line):
        yield key, line


if __name__ == "__main__":
    MRDataSketchLSH.run()