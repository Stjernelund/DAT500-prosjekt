#! /usr/bin/python3

from mrjob.job import MRJob
from mrjob.step import MRStep
import re


class MRPreProcess(MRJob):
    def steps(self):
        return [MRStep(mapper_init=self.mapper_init, mapper=self.mapper)]

    def mapper_init(self):
        self.message_id = None
        self.in_body = False
        self.body = []

    def mapper(self, _, line):
        line = line.strip()

        # Check for message ID
        if line and line[0] == '"' and line[1].isdigit():  # Short circuits if empty
            split_indices = []
            can_split = True
            # Finds all commas outside of quotes, these are the indices to split on
            for ind, char in enumerate(line):
                if char == '"':
                    can_split = not can_split
                if can_split and char == ",":
                    split_indices.append(ind)

            # The message id are the numbers before the first comma
            id_split = split_indices[0]
            message_id = line[0:id_split]
            message_id = "".join([i for i in message_id if i.isdigit()])

            if message_id:
                self.message_id = message_id
                title_temp = line[split_indices[3] + 1 : split_indices[4]]
                title = "".join(
                    [i for i in title_temp if i.isalnum() or i == " "]
                ).lower()
                self.body.append(title)

                if line.find("<AbstractText") != -1:
                    start_index = line.find("<AbstractText")
                    line = line[start_index:]
                    start_index = line.find(">") + 1
                    end_index = line.rfind("</AbstractText>")
                    line = line[start_index:end_index]
                    line = re.sub("<[^>]+>", "", line)
                    abstract = "".join(
                        [i for i in line if i.isalnum() or i == " "]
                    ).lower()
                    self.body.append(abstract)
                    yield self.message_id, " ".join(self.body)
                    self.message_id = None
                    self.body = []

                else:
                    self.in_body = True

        # Check for Abstract Text
        elif self.in_body and line.find("<AbstractText") != -1:
            start_index = line.find(">") + 1
            end_index = line.find("<", start_index)
            abstract_temp = line[start_index:end_index]
            abstract = "".join(
                [i for i in abstract_temp if i.isalnum() or i == " "]
            ).lower()
            self.body.append(abstract)

        # Check for end of Abstract
        if self.in_body and line.find("</Abstract>") != -1:
            yield self.message_id, " ".join(self.body)
            self.message_id = None
            self.body = []
            self.in_body = False


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
