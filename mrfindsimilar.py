#!/usr/bin/env python3
# -*-coding:utf-8 -*

from mrjob.job import MRJob
from mrjob.step import MRStep


class FindSimilar(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper)]

    def mapper(self, lsh, mrjobs):
        """Query each paper against the others looking for similarities"""
        similar = {}
        for key, job in mrjobs:
            found = lsh.query(job)
            found.remove(key)
            if found:
                similar[key] = found
        for key, line in similar.items():
            yield key, line


if __name__ == "__main__":
    MRFindSimilar.run()
