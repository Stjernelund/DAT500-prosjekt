#! /usr/bin/python3

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRAnalysis(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_count, reducer=self.reducer_count)]

    def mapper_count(self, key, line):
        yield None, line.count("\\")

    def reducer_count(self, _, values):
        yield None, sum(values) / 2  # because there are 2 slash per id


if __name__ == "__main__":
    MRAnalysis.run()
