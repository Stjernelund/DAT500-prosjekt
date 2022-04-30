from pyspark.ml.feature import  IDF, Tokenizer,CountVectorizer
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
import numpy as np
import os
from sklearn.feature_extraction import text
import time




if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .config('spark.executor.memory', '6g')\
        .config("spark.memory.offHeap.enabled",True)\
        .config("spark.memory.offHeap.size","50g") \
        .getOrCreate()

    sc = spark.sparkContext
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
    try:
        path ="hdfs://namenode:9000/preprocess2/part-*"
        df1 = spark.read.text(path)
        df1 = df1.withColumn("paper_id", f.split(f.col("value"), "\\t").getItem(0)).withColumn("text", f.split(f.col("value"), "\\t").getItem(1))
        df1 = df1.select(f.split(df1.value,"\\t")).rdd.flatMap(lambda x: x).toDF(schema=["paper_id","text"])
    except EOFError as x:
        print("feil på lesing")
    def dummy_fun(doc):
        return doc
    tokenizer = Tokenizer().setInputCol("text").setOutputCol("words")
    wordsData = tokenizer.transform(df1)
    corpus = wordsData.select('words').rdd.flatMap(lambda x: x).collect()
    paper_ids = wordsData.select('paper_id').rdd.flatMap(lambda x: x).collect()
    paper_ids = [id.strip('"') for id in paper_ids]
    my_stop_words = text.ENGLISH_STOP_WORDS
    mllib_times = []
    sklearn_times = []
    #Spark.ml.feature implementation of IDF
    for i in range(10):
        start = time.time()
        vectorizer = CountVectorizer(inputCol='words', outputCol='vectorizer',vocabSize=1000,minDF=0.5).fit(wordsData)
        tf = vectorizer.transform(wordsData)
        idf = IDF(inputCol="vectorizer", outputCol="tfidf_features")
        idf_model = idf.fit(tf)
        idf_data = idf_model.transform(tf)
        mlib_time = time.time() - start
        mllib_times.append(mlib_time)
    
    for i in range(10):
        start_s = time.time()
        tfidfVectorizer = TfidfVectorizer(norm=None,analyzer='word',
            tokenizer=dummy_fun,preprocessor=dummy_fun,token_pattern=None,stop_words=my_stop_words,max_features=1000, min_df=0.5)
        tf=tfidfVectorizer.fit_transform(corpus)
        tf_df=pd.DataFrame(tf.toarray(), columns = tfidfVectorizer.get_feature_names_out(),index = paper_ids)
        sklearn_time = time.time() - start_s
        sklearn_times.append(sklearn_time)
    cols = [col for col in tf_df.columns if all(char.isdigit() for char in col)]
    tf_df = tf_df.drop(cols,axis=1)
    mean = tf_df.mean(axis=0)
    print(mean.nlargest(20))
    print(tf_df.tail(12))
    print(f"mlib_time {mllib_times}")
    print(f"sklearn_time {sklearn_times}")

    #tf_df.to_csv('hdfs://namenode:9000/spark/tf-idf.csv',index = True,index_label='paper_id')
    spark.stop()