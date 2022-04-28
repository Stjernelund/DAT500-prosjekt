#!/usr/bin/python
# -*-coding:utf-8 -*

from mrjob.job import MRJob
from mrjob.step import MRStep


class Similar(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, key, line):
        yield None, 1

    def reducer(self, key, values):
        yield "Similar:", sum(values)
