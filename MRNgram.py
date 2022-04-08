#! /usr/bin/python3

from mrjob.job import MRJob
from mrjob.step import MRStep
import nltk
import sys


class MRNgram(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
            )
        ]

    def mapper(self, _, line):
        yield str(type(line)), None
        paper_id, line = line.split("\t")
        words = line.split()
        ngrams = set(nltk.ngrams(words, 5))
        for word in ngrams:
            yield paper_id, word

    def combiner(self, paper_id, words):
        yield paper_id, list(words)


if __name__ == "__main__":
    MRNgram.run()