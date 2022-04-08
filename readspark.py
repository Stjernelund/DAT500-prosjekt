from mailbox import linesep
from os import sep
from pyspark.sql.functions import split
from pyspark.sql.functions import col
import pyspark.sql.functions as f


sc = spark.sparkContext

# A text dataset is pointed to by path.
# The path can be either a single text file or a directory of text files
path = "preprocess"

df = spark.read.text(path)
#df.withColumn("paper_id", split(col("value"), "\\t").getItem(0)).withColumn("text", split(col("text"), "\\t").getItem(1)).show(false)
df = df.withColumn("paper_id", split(col("value"), "\\t").getItem(0)).withColumn("text", split(col("value"), "\\t").getItem(1))
df = df.select(f.split(df.value,"\\t")).rdd.flatMap(lambda x: x.split(" ")).toDF(schema=["paper_id","text"])
#df['text'] = df.select('text').map(lambda line: line.split(" "))

df.show()