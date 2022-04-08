from mailbox import linesep


sc = spark.sparkContext

# A text dataset is pointed to by path.
# The path can be either a single text file or a directory of text files
path = "preprocess"

df1 = spark.read.text(path, linesep = "\t") 
df1.show()