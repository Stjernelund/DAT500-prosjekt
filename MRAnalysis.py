#! /usr/bin/python3

from mrjob.job import MRJob
from mrjob.step import MRStep
import ast


class Total(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper)]

    def mapper(self, _, list_papers):
        yield None, list_papers
        # yield "Total:", len(list_papers)


class Similar(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, key, line):
        yield None, 1

    def reducer(self, key, values):
        yield "Similar:", sum(values)


class SumSimilar(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper)]

    def mapper(self, _, line):
        key, line = line.split("\t")
        key = key.strip('\\"')
        line = ast.literal_eval(line)
        yield key, len(line)


if __name__ == "__main__":
    Total.run()
    Similar.run()
    SumSimilar.run()
