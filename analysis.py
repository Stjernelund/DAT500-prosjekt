#! /usr/bin/python3

import json


def main():
    analyse()


def analyse():
    with open("similar.json", "r") as input:
        data = json.load(input)
    print(f"Total number of papers with similarity: {len(data.keys()), len(data)}")

    with open("outputB2/part-00000", "r") as input:
        counter = 0
        for line in input:
            amount = line.count("\\")
            counter += amount
        total = counter / 2
        print(f"Total number of papers: {total}.")

    print(f"Percentage of papers with similarity: {len(data.keys()) / counter * 100}%.")


if __name__ == "__main__":
    main()
