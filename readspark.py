from mailbox import linesep
from os import sep

sc = spark.sparkContext

val spark = this.spark
import spark.implicits._
# A text dataset is pointed to by path.
# The path can be either a single text file or a directory of text files
path = "preprocess"

df = spark.read.text(path)
df.withColumn("paper_id", split(col("text"), "\\t").getItem(0)).withColumn("text", split(col("text"), "\\t").getItem(1)).show(false)
df.show()