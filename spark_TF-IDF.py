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


    tfidfVectorizer = TfidfVectorizer(norm=None,analyzer='word',
                                tokenizer=dummy_fun,preprocessor=dummy_fun,token_pattern=None)
    tf=tfidfVectorizer.fit_transform(corpus)
    tf_df=pd.DataFrame(tf.toarray(), columns = tfidfVectorizer.get_feature_names_out(),index = paper_ids )
    print(tf_df.head())

    #sparktf_df=spark.createDataFrame(tf_df) 
    #sparktf_df.write.csv("/home/DAT500-prosjekt/spark_output/tf_dfcsv")
    #spark.sql.debug.maxToStringFields
    tf_df.to_csv("/home/DAT500-prosjekt/spark_output/tf_dfcsv",index = False)
    spark.stop()

    # except EOFError as x:
    #     print("første")
    # print('1')
    # try:
    #     idf = IDF(inputCol="vectorizer", outputCol="tfidf_features")
    #     idf_model = idf.fit(wordsData)
    #     wordsData = idf_model.transform(wordsData)
    # except EOFError as x:
    #     print("andre")
    # print("2")

    # def to_dense(in_vec):
    #     return DenseVector(in_vec.toArray())

    # try:
    #     to_dense_udf = f.udf(lambda x: to_dense(x), VectorUDT())
    #     wordsData = wordsData.withColumn("tfidf_features_dense", to_dense_udf('tfidf_features'))
    # except EOFError as x:
    #     print("tredje")
    
    # print("3")

    # try:
    #     wordsData_pandas = wordsData.to_pandas_on_spark(index_col = "paper_id")
    # except EOFError as x:
    #     print("fjerde")

    # print("4")


    # def dummy_fun(doc):
    #     return doc
    
    # try:
    #     tfidf = TfidfVectorizer(
    #         analyzer='word',
    #         tokenizer=dummy_fun,
    #         preprocessor=dummy_fun,
    #         token_pattern=None) 
    # except EOFError as x:
    #     print("femte")

    # print("5")

    # try:
    #     wordsData_pandas.columns = wordsData_pandas.columns.astype(str).str.strip()
    #     wordsData_pandas = wordsData_pandas.iloc[:100,:]
    # except EOFError as x:
    #     print("stopper her")
    
    # print(wordsData_pandas.head(2))



    # print("her")
    # try:
    #     print(type(wordsData_pandas))
    #     print(wordsData_pandas.shape)
    #     print(wordsData_pandas.columns)
    #     arr = wordsData_pandas['words'].to_numpy()
    #     feature_matrix = tfidf.fit_transform(arr)
    # except EOFError as x:
    #     print("sjette")
    # print("6")

    # sklearn_tfifdf = pd.DataFrame(feature_matrix.toarray(), columns=tfidf.get_feature_names())
    # spark_tfidf = pd.DataFrame([np.array(i) for i in wordsData_pandas.tfidf_features_dense], columns=vectorizer.vocabulary)




    # tfidfVectorizer = TfidfVectorizer(norm=None,analyzer='word',
    #                             tokenizer=dummy_fun,preprocessor=dummy_fun,token_pattern=None)

    # print("er her")
    # tf = tfidfVectorizer.fit_transform(df2)
    # print("2")
    # tf_df = pd.DataFrame(tf.toarray(),columns= tfidfVectorizer.get_feature_names_out())
    # print(tf_df)
    # spark.stop()
    # try:
    #     tokenizer = Tokenizer(inputCol="text", outputCol="words")
    #     wordsData = tokenizer.transform(df1)
    # except EOFError as x:
    #     print("failed first")
    
    # try:
    #     hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=100)
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

    # print("2")
    # wordsData = rescaledData.withColumn("tfidf_features_dense", to_dense_udf('features'))
    # print("3")
    # wordsData.show()
    # spark.stop()
