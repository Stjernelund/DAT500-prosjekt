from mrjob.job import MRJob
import nltk


class MRNgram(MRJob):
    def mapper(self, paper_id, body):
        yield paper_id, set(nltk.ngrams(body.split(), 2))

    def reduce(self, paper_id, ngrams):
        yield paper_id, list(ngrams)


if __name__ == '__main__':
    MRNgram.run()