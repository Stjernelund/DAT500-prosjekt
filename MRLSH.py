#! /usr/bin/python3

import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
from MinHash import GetSignatureMatrix
import json
import LSH


class MRLSH(MRJob):
    def steps(self):
        return [MRStep(reducer=self.reducer_matrix), MRStep(reducer=self.reduer_LSH)]

    def reducer_matrix(self, _, binary_matrix):
        remove = ["n", "u", "l", "\t"]
        for b in binary_matrix:
            b = "".join(c for c in b if not c in remove)
            yield None, GetSignatureMatrix(json.loads(b))

    def reduer_LSH(self, _, signature_matrix):
        lsh = LSH.LSH(1)
        for signature in signature_matrix:
            lsh.add_hash(signature)
        yield None, lsh.buckets


if __name__ == "__main__":
    MRLSH.run()
