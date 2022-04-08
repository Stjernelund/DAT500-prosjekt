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
                # reducer=self.reducer,
            )
        ]

    def mapper(self, _, line):
        paper_id, line = line.split("\t")
        yield paper_id, line

    def combiner(self, paper_id, line):
        yield str(type(line)), line
        """
        words = line.split()
        ngrams = set(nltk.ngrams(words, 5))
        for word in ngrams:
            yield paper_id, word
        """

    def reducer(self, paper_id, words):
        yield paper_id, list(words)


if __name__ == "__main__":
    MRNgram.run()
