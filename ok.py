c = 0
with open("output2_t20/part-00000", "r") as f:
    for line in f:
        if line.index("null") != -1:
            c += 1
print(c)
