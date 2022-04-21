#! /usr/bin/python3

from mrjob.job import MRJob
from mrjob.step import MRStep
import ast


class Total(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper)]

    def mapper(self, _, papers):
        _, papers = papers.split("\t")
        papers = ast.literal_eval(papers)
        yield "Total:", len(papers)


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
