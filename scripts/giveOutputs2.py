import pandas as pd
import os


def getNames(fileName):
    tempFile = open(fileName, "r")
    temp = tempFile.readlines()
    tempFile.close()
    names = []
    for elem in temp:
        elem = elem.replace("\n", "")
        elem = elem.split(".")[0]
        names.append(elem)
    return names


if __name__ == "__main__":
    # read a file and create a dataframe
    paramsNames = getNames("params.names")
    drheapNames = getNames("drheap.names")
    nonheapNames = getNames("nonheap.names")
    heapNames = getNames("heap.names")
    runNames = getNames("run.names")
    jvmcpuNames = getNames("jvmcpu.names")
    cpuNames = getNames("cpu.names")
    outputs = []

    c, h, n, r, p, j, d = 0, 0, 0, 0, 0, 0, 0

    while c < len(cpuNames) and h < len(heapNames) and n < len(nonheapNames) and r < len(runNames) and p < len(paramsNames) and j < len(jvmcpuNames) and d < len(drheapNames):

        if runNames[r] == cpuNames[c] == heapNames[h] == nonheapNames[n] == paramsNames[p] == jvmcpuNames[j] == drheapNames[d]:
            outputs.append(runNames[r])
            r += 1
            c += 1
            h += 1
            n += 1
            p += 1
            j += 1
            d += 1
        else:
            minimum = min(runNames[r], cpuNames[c], heapNames[h],
                          nonheapNames[n], paramsNames[p], jvmcpuNames[j], drheapNames[d])
            if minimum == "app-20220917055449-4110":
                print("app-20220917055449-4110")
                pass
            if minimum == runNames[r]:
                r += 1
            elif minimum == cpuNames[c]:
                c += 1
            elif minimum == heapNames[h]:
                h += 1
            elif minimum == nonheapNames[n]:
                n += 1
            elif minimum == paramsNames[p]:
                p += 1
            elif minimum == jvmcpuNames[j]:
                j += 1
            elif minimum == drheapNames[d]:
                d += 1

    for output in outputs:
        print(output)
