#! /usr/bin/python3

import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
from MinHash import GetSignatureMatrix
import json

class MRLSH(MRJob):
    def steps(self):
        return [
            MRStep(reducer=self.reducer_matrix),
        ]
    def reducer_matrix(self, _, binary_matrix):
        #yield None, GetSignatureMatrix(list(np.matrix(binary_matrix)))
        remove = ['n', 'u', 'l', '\t']
        for b in binary_matrix:
            b = ''.join(c for c in b if not c in remove)
            yield None, GetSignatureMatrix(json.loads(b)[0])


if __name__ == '__main__':
    MRLSH.run()
