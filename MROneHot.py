#! /usr/bin/python3

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
from scipy.sparse import csr_matrix
import re

class MROneHot(MRJob):
    def steps(self):
        return [
            MRStep(reducer = self.reducer_onehot)
        ]

    def reducer_onehot(self, paper_ids, ngrams):
        vocabulary = dict()
        indices = list()
        sparse_data = list()
        indptr = [0]
        for matrix in ngrams:
            flat_1 = re.findall(r"\[(.+?)\]", matrix)
            res = [sub.split(",") for sub in flat_1]
            for term in res:
                term = filter(None, [re.sub(r".*\(.*\).*", r"", i) for i in term])
                term = tuple(term)
                index = vocabulary.setdefault(term, len(vocabulary))
                indices.append(index)
                sparse_data.append(1)
            indptr.append(len(indices))
        sparse = csr_matrix((sparse_data, indices, indptr), dtype=int)
        yield paper_ids, sparse.toarray().tolist()

if __name__ == '__main__':
    MROneHot.run()
