#! /usr/bin/python3

from MRAnalysis import MRAnalysis
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
    print(f"Total time: {similar_time - start} seconds.")

    try:
        shutil.rmtree(f"output3_t{int(threshold * 100)}")
    except FileNotFoundError:
        pass
    analysis = MRAnalysis()
    with analysis.make_runner() as runner:
        runner._input_paths = [f"output2_t{int(threshold * 100)}/part-00000"]
        runner._output_dir = f"output3_t{int(threshold * 100)}"
        runner.run()
        for _, value in analysis.iter_output(runner.cat_output()):
            total_similar = value
            print(f"Total number of papers: {total_similar}.")
    print(f"Analysis: {time.time() - lshtime} seconds.")


if __name__ == "__main__":
    main()
