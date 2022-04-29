#!/usr/bin/env python3
# -*-coding:utf-8 -*

from mrjob.job import MRJob
from mrjob.step import MRStep
import main


class MRNgram(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
            )
        ]

    def mapper_init(self):
        pass

    def mapper(self, _, line):
        """Find ngrams on each paper"""
        return

        paper_id, line = line.split("\t")
        paper_id = paper_id.strip('\\"')
        words = line.split()
        ngrams = set(nltk.ngrams(words, 5))
        for word in ngrams:
            yield paper_id, word


if __name__ == "__main__":
    MRNgram.run()