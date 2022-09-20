import pandas as pd
import os


if __name__ == "__main__":
    # read a file and create a dataframe
    tempFile = open("params.names", "r")
    temp = tempFile.readlines()
    tempFile.close()
    paramsNames = []
    for elem in temp:
        elem = elem.replace("\n", "")
        elem = elem.split(".")[0]
        paramsNames.append(elem)

    tempFile = open("cpu.names", "r")
    temp = tempFile.readlines()
    tempFile.close()
    cpuNames = []
    for elem in temp:
        elem = elem.replace("\n", "")
        elem = elem.split(".")[0]
        cpuNames.append(elem)

    tempFile = open("heap.names", "r")
    temp = tempFile.readlines()
    tempFile.close()
    heapNames = []
    for elem in temp:
        elem = elem.replace("\n", "")
        elem = elem.split(".")[0]
        heapNames.append(elem)

    tempFile = open("non-heap.names", "r")
    temp = tempFile.readlines()
    tempFile.close()
    nonHeapNames = []
    for elem in temp:
        elem = elem.replace("\n", "")
        elem = elem.split(".")[0]
        nonHeapNames.append(elem)

    tempFile = open("run.names", "r")
    temp = tempFile.readlines()
    tempFile.close()
    runNames = []
    for elem in temp:
        elem = elem.replace("\n", "")
        elem = elem.split(".")[0]
        runNames.append(elem)

    outputs = []

    c, h, n, r, p = 0, 0, 0, 0, 0

    while c < len(cpuNames) and h < len(heapNames) and n < len(nonHeapNames) and r < len(runNames) and p < len(paramsNames):

        if runNames[r] == cpuNames[c] == heapNames[h] == nonHeapNames[n] == paramsNames[p]:
            outputs.append(runNames[r])
            r += 1
            c += 1
            h += 1
            n += 1
            p += 1

        else:
            minimum = min(runNames[r], cpuNames[c],
                          heapNames[h], nonHeapNames[n], paramsNames[p])
            if minimum == runNames[r]:
                r += 1
            elif minimum == cpuNames[c]:
                c += 1
            elif minimum == heapNames[h]:
                h += 1
            elif minimum == nonHeapNames[n]:
                n += 1
            elif minimum == paramsNames[p]:
                p += 1

    for output in outputs:
        print(output)
    print(len(outputs))
