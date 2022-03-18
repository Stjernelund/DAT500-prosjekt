from mrjob.job import MRJob


class MRMultilineInput(MRJob):
    def mapper_init(self):
        self.message_id = ''
        self.in_body = False
        self.stop = False

    def mapper(self, _, line):
        line = line.strip()
        if line and line[0] == '"' and line[1].isdigit():
            self.stop = False
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
                title = ''.join([i for i in title_temp if i.isalpha() or i == " "]).lower()
                self.in_body = True
                for word in title:
                    yield message_id, word

        elif line.find("<AbstractText") != -1 and self.in_body:
            startIndex = line.find(">") + 1
            endIndex = line.find("<",startIndex)
            abs_temp = line[startIndex:endIndex]
            abs = ''.join([i for i in abs_temp if i.isalpha() or i == " "]).lower()
            for word in abs:
                yield message_id, word

        elif line.find("</Abstract") != -1 and self.in_body:
            self.message_id = ''
            self.in_body = False
            self.stop = True

        elif not self.stop:
            self.in_body = True

    def combiner(self, paper_id, words):
        yield paper_id, list(words)

    def reducer(self, paper_id, words):
        yield paper_id, ''.join(words)

if __name__ == '__main__':
    MRMultilineInput.run()