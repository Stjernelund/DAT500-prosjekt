from mailbox import linesep
from os import sep
import pandas as pd

sc = spark.sparkContext

# A text dataset is pointed to by path.
# The path can be either a single text file or a directory of text files
path = "preprocess"

df = spark.read.text(path)
tmpDF = pd.DataFrame(columns=['paper_id','text'])
tmpDF[['paper_id','text']] = df['value'].str.split('\t', expand=True)
#df1 = spark.read.option("de", "\t").text(path)
tmpDF.show()