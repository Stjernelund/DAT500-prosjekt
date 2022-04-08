from mailbox import linesep
from os import sep
import sparkObject.spark.implicits._
import org.apache.spark.sql.functions.split



sc = spark.sparkContext

# A text dataset is pointed to by path.
# The path can be either a single text file or a directory of text files
path = "preprocess"

df = spark.read.text(path)
#df.withColumn("paper_id", split(col("text"), "\\t").getItem(0)).withColumn("text", split(col("text"), "\\t").getItem(1)).show(false)
df.withColumn("_tmp", split($"value", "\\t")).select(
  $"_tmp".getItem(0).as("paper_id"),
  $"_tmp".getItem(1).as("text")
)
df.show()