#! /usr/bin/python3

import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRPreProcess(MRJob):
    def steps(self):
        return [MRStep(mapper_init=self.mapper_init, mapper=self.mapper_pre)]

    def mapper_init(self):
        self.message_id = ""
        self.in_body = False
        self.body = []
        self.vocabulary = dict()
        self.indices = []
        self.sparse_data = []

    def mapper_pre(self, _, line):
        line = line.strip()
        if line and line[0] == '"' and line[1].isdigit():
            split_indices = []
            can_split = True
            for ind, c in enumerate(line):
                if c == '"':
                    can_split = not can_split
                if can_split and c == ",":
                    split_indices.append(ind)
            id_split = split_indices[0]
            message_id = line[0:id_split]
            message_id = "".join([i for i in message_id if i.isdigit()])
            if message_id != "":
                self.message_id = message_id
                title_temp = line[split_indices[3] + 1 : split_indices[4]]
                title = "".join(
                    [i for i in title_temp if i.isalnum() or i == " "]
                ).lower()
                title += " "
                self.body.append(title)
                self.in_body = True

        elif line.find("<AbstractText") != -1 and self.in_body:
            startIndex = line.find(">") + 1
            endIndex = line.find("<", startIndex)
            abs_temp = line[startIndex:endIndex]
            abs_fin = "".join([i for i in abs_temp if i.isalnum() or i == " "]).lower()
            abs_fin += " "
            self.body.append(abs_fin)

        elif line.find("</Abstract") != -1 and self.in_body:
            yield self.message_id, "".join(self.body).lower()
            self.message_id = ""
            self.body = []
            self.in_body = False

        elif line.find("<") == -1 and self.in_body:
            abs_fin = "".join([i for i in line if i.isalnum() or i == " "]).lower()
            abs_fin += " "
            self.body.append(abs_fin)


class MRNoNumerals(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        line = " ".join(word for word in line.split() if word.isalpha() or "19" in word)
        yield None, line

    def reducer(self, _, lines):
        line = " ".join(lines)
        yield None, line


if __name__ == "__main__":
    MRPreProcess.run()
