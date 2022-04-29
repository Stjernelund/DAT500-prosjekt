# DAT500-prosjekt
dataset : https://www.kaggle.com/draaslan/covid19-research-papers-dataset

# Installs
pip3 install -U scikit-learn
pip3 install numpy
pip3 install pandas
pip3 install datasketch
pip3 install nltk
pip3 install pyarrow==1.0.0

# Spark setup:
https://computingforgeeks.com/how-to-install-apache-spark-on-ubuntu-debian/


# To run preprocess and LSH algorithm with main script
python3 -r hadoop/inline threshold(float) preprocecss? (t/f)
example:
python3 main.py -r hadoop 0.5 t

# Run Spark experiment
from SPARK_HOME, run following command:

./bin/spark-submit \
  --master spark://namenode:7077 \
  /home/ubuntu/DAT500-prosjekt/spark_TF-IDF.py 

# Running times for 10 iterations  will be printed as lists in the shell

Also explain how you generate load for your system and what parameters you used here. The general idea is to include enough information for others to reproduce your experiments. To that end, you should provide a detailed set of instructions for repeating your experiments. These instructions should not be included in the report, but should be provided as part of the source code repository on GitHub, typically as the \texttt{README.md} file, or as Shell scripts or Ansible scripts.