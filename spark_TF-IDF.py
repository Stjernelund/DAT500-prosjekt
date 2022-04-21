from pyspark.ml.feature import HashingTF, IDF, Tokenizer,CountVectorizer
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.ml.linalg import VectorUDT, DenseVector
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
import numpy as np

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("TfIdfExample")\
        .config("spark.memory.offHeap.enabled","true") \
        .config("spark.memory.offHeap.size","20g") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    try:
        path = "preprocess"
        df1 = spark.read.text(path)
        df1 = df1.withColumn("paper_id", f.split(f.col("value"), "\\t").getItem(0)).withColumn("text", f.split(f.col("value"), "\\t").getItem(1))
        df1 = df1.select(f.split(df1.value,"\\t")).rdd.flatMap(lambda x: x).toDF(schema=["paper_id","text"])
    except EOFError as x:
        print("feil på lesing")
     
    tokenizer = Tokenizer().setInputCol("text").setOutputCol("words")
    wordsData = tokenizer.transform(df1)
    vectorizer = CountVectorizer(inputCol='words', outputCol='vectorizer').fit(wordsData)
    wordsData = vectorizer.transform(wordsData)

    print("er her")
    wordsData_pandas = wordsData.toPandas()
    print("er her 2")
    wordsData_pandas = wordsData_pandas[:100]

    def dummy_fun(doc):
        return doc
    
    tfidf = TfidfVectorizer(
        analyzer='word',
        tokenizer=dummy_fun,
        preprocessor=dummy_fun,
        token_pattern=None)

    feature_matrix = tfidf.fit_transform(wordsData_pandas.words)
    sklearn_tfifdf = pd.DataFrame(feature_matrix.toarray(), columns=tfidf.get_feature_names())
    spark_tfidf = pd.DataFrame([np.array(i) for i in wordsData_pandas.tfidf_features_dense], columns=vectorizer.vocabulary)

    spark_tfidf.head()
    #     # vectorize
    #     vectorizer = CountVectorizer(inputCol='words', outputCol='vectorizer').fit(wordsData)
    #     wordsData = vectorizer.transform(wordsData)

    #     # calculate scores
    #     idf = IDF(inputCol="vectorizer", outputCol="tfidf_features")
    #     idf_model = idf.fit(wordsData)
    #     wordsData = idf_model.transform(wordsData)
    #     wordsData = wordsData.limit(20)
    # except EOFError as x:
    #     print("feil på andre")

    #     # dense the current response variable
    # def to_dense(in_vec):
    #     return DenseVector(in_vec.toArray())

    # try:    
    #     to_dense_udf = f.udf(lambda x: to_dense(x), VectorUDT())
    # except EOFError as x:
    #     print("feil på to_dense")

    #     # create dense vector
    # try:
    #     wordsData = wordsData.withColumn("tfidf_features_dense", to_dense_udf('tfidf_features'))
    # except EOFError as x:
    #     print("feil på siste")

    # print((wordsData.count(), len(wordsData.columns)))
    # wordsData.show()
    # spark.stop()

    # except EOFError as x:
    #     print("failed reading")
    # try:
    #     tokenizer = Tokenizer(inputCol="text", outputCol="words")
    #     wordsData = tokenizer.transform(df1)
    # except EOFError as x:
    #     print("failed first")
    
    # try:
    #     hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
    #     featurizedData = hashingTF.transform(wordsData)
    # except EOFError as x:
    #     print("failed second")

    # try:
    #     idf = IDF(inputCol="rawFeatures", outputCol="features")
    #     idfModel = idf.fit(featurizedData)
    #     rescaledData = idfModel.transform(featurizedData)

    # except EOFError as x:
    #     print("failed third")
    
    # def to_dense(in_vec):
    #     return DenseVector(in_vec.toArray())

    # print("i am here")
    # print((rescaledData.count(), len(rescaledData.columns)))
    
    # to_dense_udf = f.udf(lambda x: to_dense(x), VectorUDT())
    # wordsData = rescaledData.withColumn("tfidf_features_dense", to_dense_udf('features'))

    # wordsData.show()
    # spark.stop()