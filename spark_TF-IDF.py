from mailbox import linesep
from pyspark.mllib.feature import HashingTF, IDF
# Load documents (one per line).
sc = spark.sparkContext

# A text dataset is pointed to by path.
# The path can be either a single text file or a directory of text files
path = "preprocess"

df1 = spark.read.text(path, linesep = "\t") 

hashingTF = HashingTF()
tf = hashingTF.transform(df1)

# While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
# First to compute the IDF vector and second to scale the term frequencies by IDF.
tf.cache()
idf = IDF().fit(tf)
tfidf = idf.transform(tf)

# spark.mllib's IDF implementation provides an option for ignoring terms
# which occur in less than a minimum number of documents.
# In such cases, the IDF for these terms is set to 0.
# This feature can be used by passing the minDocFreq value to the IDF constructor.
idfIgnore = IDF(minDocFreq=2).fit(tf)
tfidfIgnore = idfIgnore.transform(tf)