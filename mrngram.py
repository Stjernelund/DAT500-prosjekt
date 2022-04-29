#!/usr/bin/env python3
# -*-coding:utf-8 -*

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRNgram(MRJob):
    nltk = None

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
            )
        ]

    def mapper(self, _, line):
        """Find ngrams on each paper"""
        return

        paper_id, line = line.split("\t")
        paper_id = paper_id.strip('\\"')
        words = line.split()
        ngrams = set(nltk.ngrams(words, 5))
        for word in ngrams:
            yield paper_id, word

    def set_nltk(self, nltk):
        nltk = nltk


if __name__ == "__main__":
    MRNgram.run()
