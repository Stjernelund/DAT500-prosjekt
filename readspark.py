from mailbox import linesep
from os import sep


sc = spark.sparkContext

# A text dataset is pointed to by path.
# The path can be either a single text file or a directory of text files
path = "preprocess"

#df1 = spark.read.text(path, sep = "\t") 
df1 = spark.read.option("delimiter", "\\t").text(path)
df1.show()