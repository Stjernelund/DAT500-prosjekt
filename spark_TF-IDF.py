from mailbox import linesep
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql.functions import split
from pyspark.sql.functions import col
import pyspark.sql.functions as f

# Load documents (one per line).
sc = spark.sparkContext

# A text dataset is pointed to by path.
# The path can be either a single text file or a directory of text files
path = "preprocess"

df1 = spark.read.text(path).map(lambda line: line.split(" "))
df1 = df1.select(f.split(df1.value,"\\t")).rdd.flatMap(lambda x: x).toDF(schema=["paper_id","text"])

tokenizer = Tokenizer(inputCol="text", outputCol="words")
wordsData = tokenizer.transform(df1)

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)
# alternatively, CountVectorizer can also be used to get term frequency vectors

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

rescaledData.select("paper_id", "features").show()