from pyspark.ml.feature import HashingTF, IDF, Tokenizer
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

# Load documents (one per line).
spark = SparkSession\
    .builder\
    .appName("TfIdfExample")\
    .getOrCreate()

# A text dataset is pointed to by path.
# The path can be either a single text file or a directory of text files
path = "preprocess"
df1 = spark.read.text(path)
df1 = df1.withColumn("paper_id", f.split(f.col("value"), "\\t").getItem(0)).withColumn("text", f.split(f.col("value"), "\\t").getItem(1))
df1 = df1.select(f.split(df1.value,"\\t")).rdd.flatMap(lambda x: x).toDF(schema=["paper_id","text"])

tokenizer = Tokenizer(inputCol="text", outputCol="words")
wordsData = tokenizer.transform(df1)

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures",numFeatures=20)
featurizedData = hashingTF.transform(wordsData)

idf = IDF(inputCol="rawFeatures" , outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)
rescaledData.select("paper_id", "words","features").show()
spark.stop()
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