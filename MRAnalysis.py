#! /usr/bin/python3

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRAnalysis(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_count, reducer=self.reducer_count)]

    def mapper_count(self, _, line):
        yield None, 1

    def reducer_count(self, _, values):
        yield None, str(list(values))


if __name__ == "__main__":
    MRAnalysis.run()
