from mrjob.job import MRJob
import nltk


class MRNgram(MRJob):
    def mapper(self, _, line):
        yield set(nltk.ngrams(line.split(), 2))


if __name__ == '__main__':
    MRNgram.run()