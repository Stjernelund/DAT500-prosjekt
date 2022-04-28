#!/usr/bin/python
# -*-coding:utf-8 -*

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
