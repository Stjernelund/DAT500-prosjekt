#! /usr/bin/python3

import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep
import nltk
import sys
from scipy.sparse import csr_matrix

class MRLSH(MRJob):
    def steps(self):
        return [
            MRStep(mapper_init = self.mapper_init, mapper=self.mapper_pre),
            MRStep(mapper = self.mapper_ngram, reducer=self.reducer_ngram),
            MRStep(mapper_init = self.mapper_init_onehot, mapper = self.mapper_onehot, reducer=self.reducer_onehot)
        ]
    def mapper_init(self):
        self.message_id = ''
        self.in_body = False
        self.body = []
        self.vocabulary = dict()
        self.indices = list()
        self.sparse_data = list()

    def mapper_pre(self, _, line):
        line = line.strip()
        if line and line[0] == '"' and line[1].isdigit():
            split_indices = []
            can_split = True
            for ind, c in enumerate(line):
                if c == '"':
                    can_split = not can_split
                if can_split and c == ',':
                    split_indices.append(ind)
            id_split = split_indices[0]
            message_id = line[0:id_split]
            message_id = ''.join([i for i in message_id if i.isdigit()])
            if message_id != '':
                self.message_id = message_id
                title_temp = line[split_indices[3] + 1:split_indices[4]]
                title = ''.join([i for i in title_temp if i.isalnum() or i == " "]).lower()
                self.body.append(title)
                self.in_body = True

        elif line.find("<AbstractText") != -1 and self.in_body:
            startIndex = line.find(">") + 1
            endIndex = line.find("<", startIndex)
            abs_temp = line[startIndex:endIndex]
            abs = ''.join([i for i in abs_temp if i.isalnum() or i == " "]).lower()
            self.body.append(abs)

        elif line.find("</Abstract") != -1 and self.in_body:
            yield self.message_id, ''.join(self.body).lower()
            self.message_id = ''
            self.body = []
            self.in_body = False

        elif line.find('<') == -1 and self.in_body:
            abs = ''.join([i for i in line if i.isalnum() or i == " "]).lower()
            self.body.append(abs)

    def mapper_init_onehot(self):
        self.vocabulary = dict()
        self.indices = list()
        self.sparse_data = list()
        self.indptr = [0]

    def mapper_ngram(self, paper_id, text):
        splits = text.split()
        ngrams = set(nltk.ngrams(splits, 2))
        for word in ngrams:
            yield paper_id, word

    def reducer_ngram(self, paper_id, words):
        yield paper_id, list(words)

    def mapper_onehot(self, paper_id, ngrams):
        for term in ngrams:
            index = self.vocabulary.setdefault(term, len(self.vocabulary))
            self.indices.append(index)
            self.sparse_data.append(1)
        self.indptr.append(len(self.indices))
        yield paper_id, ngrams
    
    def reducer_onehot(self, paper_id, ngrams):
        sparse = csr_matrix((self.sparse_data, self.indices, self.indptr), dtype=int)

if __name__ == '__main__':
    MRLSH.run()
