#!/usr/bin/env python3
# -*-coding:utf-8 -*

from mrtotal import Total
from mrpreprocess import MRPreProcess
from mrlsh import DataSketchLSH
from mrngram import MRNgram
from mrsimilar import Similar
from mrsumsimilar import SumSimilar
from mrfindsimilar import FindSimilar
import time
from datetime import datetime
import shutil
import sys
import os


def main():
    start = time.time()
    print("Started at:", datetime.now().strftime("%H:%M:%S"))

    # Run by using the following command: python3 main.py [-r hadoop] threshold_value true/false
    preprocess = "t" in sys.argv[3].lower()
    run_hadoop = "hadoop" in sys.argv[2].lower()
    hadoop_string = "hdfs://" if run_hadoop else f""

    if preprocess:
        # Remove the previous output directory
        try:
            if run_hadoop:
                os.system("hdfs dfs -rm -r /preprocess")
            else:
                shutil.rmtree("preprocess")
        except FileNotFoundError:
            pass
        start = time.time()
        preprocesser = MRPreProcess()
        with preprocesser.make_runner() as runner:
            runner._input_paths = [f"{hadoop_string}/papers.csv"]
            runner._input_paths = ["papers.csv"]
            runner._output_dir = f"{hadoop_string}/preprocess"
            runner.run()

    preprostime = time.time()
    print(f"Preprocessing: {preprostime - start} seconds.")

    if preprocess:
        try:
            if run_hadoop:
                os.system("hdfs dfs -rm -r /ngrams")
            else:
                shutil.rmtree("ngrams")
        except FileNotFoundError:
            pass
        ngrams = MRNgram()
        with ngrams.make_runner() as runner:
            runner._input_paths = [f"{hadoop_string}/preprocess"]
            runner._output_dir = f"{hadoop_string}/ngrams"
            runner.run()

    ngramtime = time.time()
    print(f"Ngrams: {ngramtime - preprostime} seconds.")

    ds = DataSketchLSH()

    threshold = ds.threshold
    path = f"output_t{int(threshold * 100)}"

    # Remove the previous output directory
    try:
        if run_hadoop:
            os.system(f"hdfs dfs -rm -r /{path}")
        else:
            shutil.rmtree(f"{path}")
    except FileNotFoundError:
        pass

    with ds.make_runner() as runner:
        runner._input_paths = [f"{hadoop_string}/ngrams"]
        runner._output_dir = f"{hadoop_string}/{path}/lsh"
        runner.run()

    lshtime = time.time()
    print(f"LSH: {lshtime - ngramtime} seconds.")

    """
    find_similar = FindSimilar()
    find_similar.init(lsh, ds.mrjobs)
    with find_similar.make_runner() as runner:
        runner._input_paths = [f"{hadoop_string}/{path}/lsh"]
        runner._output_dir = f"{hadoop_string}/{path}/similars"
        runner.run()

    ds.find_similar(lsh, mrjobs)
    similar_time = time.time()
    print(f"Similarity: {similar_time - lshtime} seconds.")

    MR_total = Total()
    with MR_total.make_runner() as runner:
        runner._input_paths = [f"{hadoop_string}/{path}/lsh"]
        runner._output_dir = f"{hadoop_string}/{path}/total"
        runner.run()
        for _, value in MR_total.parse_output(runner.cat_output()):
            total = value
            print(f"Total number of papers: {total}.")

    MR_similar = Similar()
    with MR_similar.make_runner() as runner:
        runner._input_paths = [f"{hadoop_string}/{path}/similars"]
        runner._output_dir = f"{hadoop_string}/{path}/similar_total"
        runner.run()
        for _, value in MR_similar.parse_output(runner.cat_output()):
            similar = value
            print(f"Number of similar papers: {similar}.")
            print(f"Similarity: {similar / total * 100}%.")

    sum_similar = SumSimilar()
    with sum_similar.make_runner() as runner:
        runner._input_paths = [f"{hadoop_string}/{path}/similars"]
        runner._output_dir = f"{hadoop_string}/{path}/similar_sum"
        runner.run()

    print(f"Analysis: {time.time() - lshtime} seconds.")
    print(f"Total time: {time.time() - start} seconds.")

    """


if __name__ == "__main__":
    main()
