#!/usr/bin/env python3
# -*-coding:utf-8 -*

from mrjob.job import MRJob
from mrjob.step import MRStep


class FindSimilar(MRJob):
    def init(self, lsh, mrjob):
        self.lsh = lsh
        self.mrjob = mrjob

    def steps(self):
        return [MRStep(mapper=self.mapper)]

    def mapper(self):
        """Query each paper against the others looking for similarities"""
        similar = {}
        print(similar)
        for key, job in self.mrjobs:
            found = self.lsh.query(job)
            found.remove(key)
            if found:
                similar[key] = found
        print(similar)
        for key, line in similar.items():
            yield key, line


if __name__ == "__main__":
    FindSimilar.run()
