from pyspark.ml.feature import  IDF, Tokenizer,CountVectorizer
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
import numpy as np
import os
from sklearn.feature_extraction import text




if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .config('spark.executor.memory', '6g')\
        .config('spark.sql.shuffle.partitions', '200')\
        .config("spark.memory.offHeap.enabled",True)\
        .config("spark.memory.offHeap.size","50g") \
        .getOrCreate()

    sc = spark.sparkContext
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
    try:
        path ="hdfs://namenode:9000/preprocess/output2/part-*"
        df1 = spark.read.text(path)
        df1 = df1.withColumn("paper_id", f.split(f.col("value"), "\\t").getItem(0)).withColumn("text", f.split(f.col("value"), "\\t").getItem(1))
        df1 = df1.select(f.split(df1.value,"\\t")).rdd.flatMap(lambda x: x).toDF(schema=["paper_id","text"])
    except EOFError as x:
        print("feil på lesing")
    
    tokenizer = Tokenizer().setInputCol("text").setOutputCol("words")
    wordsData = tokenizer.transform(df1)
    vectorizer = CountVectorizer(inputCol='words', outputCol='vectorizer').fit(wordsData)
    wordsData = vectorizer.transform(wordsData)
    wordsData.cache()
    #Spark.ml.feature implementation of IDF
    idf = IDF(inputCol="vectorizer", outputCol="tfidf_features")
    idf_model = idf.fit(wordsData)
    idf_data = idf_model.transform(wordsData)

    wordsData_pandas = wordsData.to_pandas_on_spark()
    wordsData_pandas.set_index('paper_id')
    corpus = vectorizer.vocabulary
    #corpus = wordsData_pandas.words
    #corpus = corpus.tolist()
    corpus = [[word.strip('"') for word in sublist] for sublist in corpus]
    # paper_ids = wordsData_pandas.paper_id
    # paper_ids = paper_ids.tolist()
    # paper_ids = [id.strip('"') for id in paper_ids]

    
    def dummy_fun(doc):
        return doc
    my_stop_words = text.ENGLISH_STOP_WORDS

    tfidfVectorizer = TfidfVectorizer(norm=None,analyzer='word',
        tokenizer=dummy_fun,preprocessor=dummy_fun,token_pattern=None,stop_words=my_stop_words)
    tf=tfidfVectorizer.fit_transform(corpus)
    tf_df=pd.DataFrame(tf.toarray(), columns = tfidfVectorizer.get_feature_names_out(),index = wordsData_pandas.index)
    
    print(tf_df.tail(12))

    # tf_df.to_csv("hdfs://namenode:9000/preprocess",index = True,index_label='paper_id')
    spark.stop()