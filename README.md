# DAT500-prosjekt
dataset : https://www.kaggle.com/draaslan/covid19-research-papers-dataset

# Installs
pip install -U scikit-learn
pip install numpy
pip install pandas
pip install datasketch
pip install nltk
pip install -U scikit-learn
pip install pandas

# Spark setup:
https://computingforgeeks.com/how-to-install-apache-spark-on-ubuntu-debian/


# To run preprocess and LSH algorithm with main script
python3 -r hadoop/inline threshold(float) preprocecss? (t/f)
example:
python3 main.py -r hadoop 0.5 t

# Run Spark
./bin/spark-submit \
  --master spark://namenode:7077 \
  /home/ubuntu/DAT500-prosjekt/spark_TF-IDF.py 

Also explain how you generate load for your system and what parameters you used here. The general idea is to include enough information for others to reproduce your experiments. To that end, you should provide a detailed set of instructions for repeating your experiments. These instructions should not be included in the report, but should be provided as part of the source code repository on GitHub, typically as the \texttt{README.md} file, or as Shell scripts or Ansible scripts.