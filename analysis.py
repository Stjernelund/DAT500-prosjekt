#! /usr/bin/python3

import json


def main():
    analyse()


def analyse():
    with open("similarity.json", "w+") as input:
        data = json.load(input)
    print(f"Total number of papers with similarity: {len(data.keys()), len(data)}")


if __name__ == "__main__":
    main()
