#!/usr/bin/python
# -*-coding:utf-8 -*

from mrjob.job import MRJob
from mrjob.step import MRStep
import nltk


class MRNgram(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
            )
        ]

    def mapper(self, _, line):
        print("test")
        """Find ngrams on each paper"""
        paper_id, line = line.split("\t")
        paper_id = paper_id.strip('\\"')
        words = line.split()
        ngrams = set(nltk.ngrams(words, 5))
        for word in ngrams:
            yield paper_id, word

    def combiner(self, paper_id, words):
        yield paper_id, list(words)


if __name__ == "__main__":
    MRNgram.run()
