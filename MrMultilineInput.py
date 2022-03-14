from email import message

from matplotlib.pyplot import title
from mrjob.job import MRJob

class MRMultilineInput(MRJob):
    def mapper_init(self):
        self.message_id = ''
        self.in_body = False
        self.body = []

    def mapper(self,_,line):
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
            message_id = line[0:id_split][2:-2]
            if message_id != "":
                self.message_id = message_id
                title_temp = line[split_indices[3] + 1:split_indices[4]]
                title_str = ''.join([i for i in title_temp if i.isalpha() or i == " "])
                self.body.append(title_str)
                self.in_body = True

        elif line.find("<AbstractText") == 0:
            startIndex = line.find(">") + 1
            endIndex = line.find("<",startIndex)
            abs_temp = line[line[startIndex:endIndex]]
            abs = ''.join([i for i in abs_temp if i.isalpha() or i == " "])
            self.body.append(abs)

        elif line.find("</Abstract") and self.in_body:
            yield self.message_id, ''.join(self.body)
            self.message_id = ''
            self.body = []
            self.in_body = False

        else:
            self.in_body = True


if __name__ == '__main__':
    MRMultilineInput.run()