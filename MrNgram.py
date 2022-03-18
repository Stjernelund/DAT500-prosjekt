#! /usr/bin/python3

from mrjob.job import MRJob
import nltk
import sys


class MRNgram(MRJob):
    def mapper_init(self):
        self.in_body = False

    def mapper(self, _, line):
        if line[0] == '"':
            self.in_body = True if line[-1] == '"' else False
            # splits = [x for x in line.split('"') if x != '' and x != '\n' and x != '\t']
            splits = line.split('"')
            if len(splits) > 1:
                paper_id = splits[1]
                for word in splits[3].split():
                    yield paper_id, word

    def combiner(self, paper_id, words):
        ngrams = set(nltk.ngrams(words, 2))
        for word in ngrams:
            yield paper_id, word

    def reducer(self, paper_id, words):
        yield paper_id, list(words)


if __name__ == '__main__':
    MRNgram.run()