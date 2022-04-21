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
df1 = spark.read.text(path)
df1 = df1.withColumn("paper_id", split(col("value"), "\\t").getItem(0)).withColumn("text", split(col("value"), "\\t").getItem(1))
df1 = df1.select(f.split(df1.value,"\\t")).rdd.flatMap(lambda x: x).toDF(schema=["paper_id","text"])

tokenizer = Tokenizer(inputCol="text", outputCol="words")
wordsData = tokenizer.transform(df1)

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
featurizedData = hashingTF.transform(wordsData)
# # alternatively, CountVectorizer can also be used to get term frequency vectors

idf = IDF(inputCol="rawFeatures" , outputCol="features")
idfModel = idf.fit(featurizedData)
#rescaledData = idfModel.transform(featurizedData).show()
rescaledData.select("label", "features").show()
# rescaledData.select("paper_id", "words","features").show()
# rescaledData.

#hashingTF = HashingTF(inputCol="text",outputCol="words")
#tf = hashingTF.transform(df1)

#tf.cache()
#idf = IDF().fit(tf)
#tfidf = idf.transform(tf)

#tfidf.show()

#idfIgnore = IDF(minDocFreq=2).fit(tf)
#tfidfIgnore = idfIgnore.transform(tf)
#for each in tfidf.collect():
#    print(each)