#! /usr/bin/python3

from mrjob.job import MRJob
from mrjob.step import MRStep


class Total(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_count, reducer=self.reducer_count)]

    def mapper_count(self, key, line):
        yield None, line.count("\\")

    def reducer_count(self, _, values):
        yield "Total:", int(sum(values) / 2)  # because there are 2 slash per id


class Similar(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper_count, reducer=self.reducer_count)]

    def mapper_count(self, key, line):
        yield None, 1

    def reducer_count(self, key, values):
        yield "Similar:", sum(values)


if __name__ == "__main__":
    Total.run()
    Similar.run()
