from pyspark.ml.feature import HashingTF, IDF, Tokenizer
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.ml.linalg import VectorUDT, DenseVector

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("TfIdfExample")\
        .getOrCreate()

    try:
        path = "preprocess"
        df1 = spark.read.text(path)
        df1 = df1.withColumn("paper_id", f.split(f.col("value"), "\\t").getItem(0)).withColumn("text", f.split(f.col("value"), "\\t").getItem(1))
        df1 = df1.select(f.split(df1.value,"\\t")).rdd.flatMap(lambda x: x).toDF(schema=["paper_id","text"])
    except EOFError as x:
        print("failed reading")
    try:
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        wordsData = tokenizer.transform(df1)
    except EOFError as x:
        print("failed first")
    
    try:
        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
        featurizedData = hashingTF.transform(wordsData)
    except EOFError as x:
        print("failed second")

    try:
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)

    except EOFError as x:
        print("failed third")
    
    def to_dense(in_vec):
        return DenseVector(in_vec.toArray())

    print("i am here")
    print((rescaledData.count(), len(rescaledData.columns)))
    
    to_dense_udf = f.udf(lambda x: to_dense(x), VectorUDT())
    wordsData = rescaledData.withColumn("tfidf_features_dense", to_dense_udf('features'))

    wordsData.show()
    spark.stop()