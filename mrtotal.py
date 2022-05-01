#!/usr/bin/env python3
# -*-coding:utf-8 -*

from mrjob.job import MRJob
from mrjob.step import MRStep
import ast


class Total(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper)]

    def mapper(self, _, papers):
        yield None, 1

    def combiner(self, _, values):
        yield None, sum(values)

    def reducer(self, _, values):
        yield "Total:", sum(values)


if __name__ == "__main__":
    Total.run()
