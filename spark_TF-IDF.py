from pyspark.ml.feature import HashingTF, IDF, Tokenizer
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

# Load documents (one per line).
# if __name__ == "__main__":
#     spark = SparkSession\
#         .builder\
#         .appName("TfIdfExample")\
#         .getOrCreate()

#     A text dataset is pointed to by path.
#     The path can be either a single text file or a directory of text files
#     try:
#         path = "preprocess"
#         df1 = spark.read.text(path)
#         df1 = df1.withColumn("paper_id", f.split(f.col("value"), "\\t").getItem(0)).withColumn("text", f.split(f.col("value"), "\\t").getItem(1))
#         df1 = df1.select(f.split(df1.value,"\\t")).rdd.flatMap(lambda x: x).toDF(schema=["paper_id","text"])
#     except EOFError as x:
#         print("failed reading")
#     try:
#         tokenizer = Tokenizer(inputCol="text", outputCol="words")
#         wordsData = tokenizer.transform(df1)
#     except EOFError as x:
#         print("failed first")
    
#     try:
#         hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
#         featurizedData = hashingTF.transform(wordsData)
#     except EOFError as x:
#         print("failed second")

#     try:
#         idf = IDF(inputCol="rawFeatures", outputCol="features")
#         idfModel = idf.fit(featurizedData)
#         rescaledData = idfModel.transform(featurizedData)
#         rescaledData.select("paper_id", "words" ,"features").show() 
#         spark.stop()

#     except EOFError as x:
#         print("failed third")
    
sentenceData = spark.createDataFrame([
    (0.0, "Hi I heard about Spark"),
    (0.0, "I wish Java could use case classes"),
    (1.0, "Logistic regression models are neat")
], ["label", "sentence"])

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)
# alternatively, CountVectorizer can also be used to get term frequency vectors

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

rescaledData.select("label", "features").show()