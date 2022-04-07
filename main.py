#! /usr/bin/python3

import MRAnalysis
from MRPreProcess import MRPreProcess
from DataSketchLSH import MRDataSketchLSH
import time
import shutil


def main():
    start = time.time()
    threshold = 0.2

    """
    try:
        shutil.rmtree("output")
    except FileNotFoundError:
        pass
    preprocesser = MRPreProcess()
    with preprocesser.make_runner() as runner:
        runner._input_paths = ["papers.csv"]
        runner._output_dir = "output"
        runner.run()
    """

    """
    preprostime = time.time()
    print(f"Preprocessing: {preprostime - start} seconds.")

    try:
        shutil.rmtree(f"output2_t{int(threshold * 100)}")
    except FileNotFoundError:
        pass

    datasketch = MRDataSketchLSH()
    datasketch.init(threshold)
    with datasketch.make_runner() as runner:
        runner._input_paths = ["output/part-*"]
        runner._output_dir = f"output2_t{int(threshold * 100)}"
        runner.run()

    minhashtime = time.time()
    print(f"Hashing: {minhashtime - preprostime} seconds.")

    lsh = datasketch.make_LSH()
    lshtime = time.time()
    print(f"LSH: {lshtime - minhashtime} seconds.")

    datasketch.find_similar(lsh)
    similar_time = time.time()
    print(f"Similarity: {similar_time - lshtime} seconds.")
    """
    try:
        shutil.rmtree(f"output3_t{int(threshold * 100)}")
    except FileNotFoundError:
        pass

    MR_total = MRAnalysis.Total()
    with MR_total.make_runner() as runner:
        runner._input_paths = [f"output2_t{int(threshold * 100)}/part-00000"]
        runner._output_dir = f"output3_t{int(threshold * 100)}"
        runner.run()
        for _, value in MR_total.parse_output(runner.cat_output()):
            total = value
            print(f"Total number of papers: {total}.")

    MR_similar = MRAnalysis.Similar()
    with MR_similar.make_runner() as runner:
        runner._input_paths = [
            f"output2_t{int(threshold * 100)}/similar_t{int(threshold * 100)}.txt"
        ]
        runner._output_dir = f"output3_t{int(threshold * 100)}"
        runner.run()
        for _, value in MR_similar.parse_output(runner.cat_output()):
            similar = value
            print(f"Number of similar papers: {similar}.")
            print(f"Similarity: {similar / total}%.")
    # print(f"Analysis: {time.time() - lshtime} seconds.")
    # print(f"Total time: {similar_time - start} seconds.")


if __name__ == "__main__":
    main()
