#! /usr/bin/python3

from mrjob.job import MRJob
from mrjob.step import MRStep
from scipy.sparse import csr_matrix
import numpy as np

class MROneHot(MRJob):
    def steps(self):
        return [
            MRStep(reducer = self.reducer_onehot)
        ]

    def reducer_onehot(self, _, ngrams):
        ngrams = np.matrix(ngrams)
        vocabulary = dict()
        indices = list()
        sparse_data = list()
        indptr = [0]
        for ngram in ngrams:
            for term in ngram:
                term = tuple(term)
                index = vocabulary.setdefault(term, len(vocabulary))
                indices.append(index)
                sparse_data.append(1)
            indptr.append(len(indices))
        sparse = csr_matrix((sparse_data, indices, indptr), dtype=int)
        yield None, sparse.toarray().tolist()


if __name__ == '__main__':
    MROneHot.run()
