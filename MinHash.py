#!/usr/bin/env python
import sys

def h1(x):
        return (x+1)%5
def h2(x):
        return (3*x+1)%5

def minhash(data, hashfuncs):
    rows, cols, sigrows = len(data), len(data[0]), len(hashfuncs)

    # initialize signature matrix with maxint
    sigmatrix = []
    for i in range(sigrows):
        sigmatrix.append([sys.maxint] * cols)

    for r in range(rows):
        hashvalue = map(lambda x: x(r), hashfuncs)
        # if data != 0 and signature > hash value, replace signature with hash value
        for c in range(cols):
            if data[r][c] == 0:
                continue
            for i in range(sigrows):
                if sigmatrix[i][c] > hashvalue[i]:
                    sigmatrix[i][c] = hashvalue[i]

    return sigmatrix

def GetSignatureMatrix(binary_matrix):
    return minhash(binary_matrix,[h1, h2])