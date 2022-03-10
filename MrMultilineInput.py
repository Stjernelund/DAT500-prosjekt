from email import message
from mrjob.job import MRJob

class MRMultilineInput(MRJob):
    def mapper_init(self):
        self.message_id = ''
        self.in_body = False
        self.body = []
    
    def mapper(self,_,line):
        line = line.strip()

        if line and line[0] == '"' and line[1].isdigit():
            line_split = line.split('"')
            line_split[:] = (value for value in line_split if value != ',')
            line_split = line_split[1:]
            self.message_id = line_split[0]
            self.body.append(line[4])
            self.in_body = True

        elif line.find("<AbstractText") == 0:
            startIndex = line.find(">") + 1
            endIndex = line.find("<",startIndex)
            self.body.append(line[startIndex:endIndex])

        elif line.find("</Abstract") and self.in_body:
            yield self.message_id, ''.join(self.body)
            self.message_id = ''
            self.body = []
            self.in_body = False
        
        else:
            self.in_body = True
            
        
if __name__ == '__main__':
    MRMultilineInput.run()