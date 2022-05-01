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
python3 -r hadoop/inline preprocecss? (t/f)
- example:
python3 main.py -r hadoop t

# Run Spark experiments
- from SPARK_HOME, run following command:

./bin/spark-submit \
  --master spark://namenode:7077 \
  /home/ubuntu/DAT500-prosjekt/spark_TF-IDF.py 

- In spark_TF-IDF.py, change parameters in CountVectorizer and TfidfVectorizer to copy the same experiments from the paper.

- Running times for 10 iterations  will be printed as lists in the shell
