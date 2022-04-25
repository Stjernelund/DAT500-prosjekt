from pyspark.ml.feature import HashingTF, IDF, Tokenizer,CountVectorizer
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
        .getOrCreate()

    sc = spark.sparkContext
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
    print(spark.sparkContext.getConf().getAll())
    print("next")
    print(f"workers: {sc._conf.get('spark.executor.instances')}")
    try:
        path = "preprocess_alpha"
        df1 = spark.read.text(path)
        df1 = df1.withColumn("paper_id", f.split(f.col("value"), "\\t").getItem(0)).withColumn("text", f.split(f.col("value"), "\\t").getItem(1))
        df1 = df1.select(f.split(df1.value,"\\t")).rdd.flatMap(lambda x: x).toDF(schema=["paper_id","text"])
    except EOFError as x:
        print("feil på lesing")


    tokenizer = Tokenizer().setInputCol("text").setOutputCol("words")
    wordsData = tokenizer.transform(df1)
    vectorizer = CountVectorizer(inputCol='words', outputCol='vectorizer').fit(wordsData)
    wordsData = vectorizer.transform(wordsData).iloc[:100,:]

    #Spark.ml.feature implementation of IDF
    idf = IDF(inputCol="vectorizer", outputCol="tfidf_features")
    idf_model = idf.fit(wordsData)
    idf_data = idf_model.transform(wordsData)
    idf_data.show()

    wordsData_pandas = wordsData.to_pandas_on_spark().iloc[:100,:]
    paper_ids = wordsData_pandas['paper_id'].to_numpy()
    wordsData_pandas.set_index('paper_id')
    corpus = wordsData_pandas['words'].to_numpy()
    paper_ids = [id.strip('"') for id in paper_ids]

    corpus = [[word.strip('"') for word in sublist] for sublist in corpus]
    paper_ids = wordsData_pandas['paper_id'].to_numpy()
    def dummy_fun(doc):
        return doc
    my_stop_words = text.ENGLISH_STOP_WORDS

    my_stop_words = text.ENGLISH_STOP_WORDS
    tfidfVectorizer = TfidfVectorizer(norm=None,analyzer='word',
                                tokenizer=dummy_fun,preprocessor=dummy_fun,token_pattern=None,stop_words=my_stop_words)
    tf=tfidfVectorizer.fit_transform(corpus)
    tf_df=pd.DataFrame(tf.toarray(), columns = tfidfVectorizer.get_feature_names_out(),index = paper_ids )
    print(tf_df.head())

    tf_df.to_csv("/home/DAT500-prosjekt/spark_output/tf_dfcsv",index = True,index_label='paper_id')
    spark.stop()

