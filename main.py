#! /usr/bin/python3

import MRAnalysis
from preprocess import MRPreProcess, MRNoNumerals
from DataSketchLSH import MRDataSketchLSH
from MRNgram import MRNgram
import time
from datetime import datetime
import shutil
import sys


def main():
    start = time.time()
    print("Started at:", datetime.now().strftime("%H:%M:%S"))

    # Run by using the following command: python3 main.py threshold_value true/false
    threshold = float(sys.argv[3])
    path = f"output_t{int(threshold * 100)}"
    preprocess = "t" in sys.argv[4].lower()
    run_hadoop = "hadoop" in sys.argv[2].lower()
    print(f"hadoop ?: {run_hadoop}")

    if preprocess:
        # Remove the previous output directory
        try:
            if run_hadoop:
                shutil.rmtree("hdfs://preprocess")
            else:
                shutil.rmtree("preprocess")
        except FileNotFoundError as e:
            print(e)
        preprocesser = MRPreProcess()
        with preprocesser.make_runner() as runner:
            if run_hadoop:
                runner._input_paths = ["hdfs://preprocess/papers.csv"]
                runner._output_dir = "hdfs://preprocess/output2"
            # run inline
            else:
                runner._input_paths = ["papers.csv"]
                runner._output_dir = "preprocess"
            runner.run()

    preprostime = time.time()
    print(f"Preprocessing: {preprostime - start} seconds.")

    if preprocess:

        try:
            shutil.rmtree("preprocess_alpha")
        except FileNotFoundError:
            pass
        no_numerals = MRNoNumerals()
        with no_numerals.make_runner() as runner:
            if run_hadoop:
                runner._input_paths = ["hdfs:///preprocess/output2/part-*"]
                runner._output_dir = "hdfs:///preprocess/output3"
            else:
                runner._input_paths = ["preprocess/part-*"]
                runner._output_dir = "preprocess_alpha"
            runner.run()

    prepro_alpha_stime = time.time()
    print(f"Preprocessing_alpha: {prepro_alpha_stime - preprostime} seconds.")

    if preprocess:
        try:
            shutil.rmtree("ngrams")
        except FileNotFoundError:
            pass
        ngrams = MRNgram()
        with ngrams.make_runner() as runner:
            if run_hadoop:
                runner._input_paths = ["hdfs:///preprocess/output2/part-*"]
                runner._output_dir = "hdfs:///ngrams/output2"
            else:
                runner._input_paths = ["preprocess/part-*"]
                runner._output_dir = "ngrams/outputs"
            runner.run()

    ngramtime = time.time()
    print(f"Ngrams: {ngramtime - prepro_alpha_stime} seconds.")

    # Remove the previous output directory
    try:
        shutil.rmtree(f"{path}")
    except FileNotFoundError:
        pass

    datasketch = MRDataSketchLSH()
    datasketch.init(threshold)
    with datasketch.make_runner() as runner:
        if run_hadoop:
            runner._input_paths = ["hdfs:///ngrams/output2/part-*"]
        else:
            runner._input_paths = ["ngrams/outputs/part-*"]
            runner._output_dir = f"{path}/lsh"
        runner.run()

    minhashtime = time.time()
    print(f"Hashing: {minhashtime - ngramtime} seconds.")

    lsh = datasketch.make_LSH()
    lshtime = time.time()
    print(f"LSH: {lshtime - minhashtime} seconds.")

    datasketch.find_similar(lsh)
    similar_time = time.time()
    print(f"Similarity: {similar_time - lshtime} seconds.")

    MR_total = MRAnalysis.Total()
    with MR_total.make_runner() as runner:
        # inline
        runner._input_paths = [f"{path}/lsh/part-*"]
        runner._output_dir = f"{path}/total"
        runner.run()
        for _, value in MR_total.parse_output(runner.cat_output()):
            total = value
            print(f"Total number of papers: {total}.")

    MR_similar = MRAnalysis.Similar()
    with MR_similar.make_runner() as runner:
        # inline
        runner._input_paths = [f"{path}/similar.txt"]
        runner._output_dir = f"{path}/similar_total"
        runner.run()
        for _, value in MR_similar.parse_output(runner.cat_output()):
            similar = value
            print(f"Number of similar papers: {similar}.")
            print(f"Similarity: {similar / total * 100}%.")

    sum_similar = MRAnalysis.SumSimilar()
    with sum_similar.make_runner() as runner:
        # inline
        runner._input_paths = [f"{path}/similar.txt"]
        runner._output_dir = f"{path}/similar_sum"
        runner.run()

    print(f"Analysis: {time.time() - lshtime} seconds.")
    print(f"Total time: {time.time() - start} seconds.")


if __name__ == "__main__":
    main()
