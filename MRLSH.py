#! /usr/bin/python3

import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep
from MinHash import GetSignatureMatrix

class MRLSH(MRJob):
    def steps(self):
        return [
            MRStep(reducer=self.reducer_matrix),
        ]
    def reducer_matrix(self, _, binary_matrix):
        yield None, GetSignatureMatrix(list(np.matrix(binary_matrix)))


if __name__ == '__main__':
    MRLSH.run()
