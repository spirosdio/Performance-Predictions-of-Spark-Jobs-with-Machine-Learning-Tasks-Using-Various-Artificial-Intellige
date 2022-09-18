

import csv
import json
import math
import os
from re import A, I


def getmax(name, before, after):
    csvaki = open(before + name+after)
    myCsv = csv.reader(csvaki)

    next(myCsv)
    sumCpu = 0
    for row in myCsv:
        sumCpu = max(int(row[1]), sumCpu)

    csvaki.close()
    return sumCpu


def maxoftalbe(a):
    if a:
        return math.floor(max([int(x) for x in a]))
    else:
        return 0


def getQuorters(name, before, after):
    csvaki = open(before + name+after)
    myCsv = csv.reader(csvaki)
    next(myCsv)

    table = []
    for row in myCsv:
        table.append(row[1])

    lengi = len(table)
    a = table[0:1*lengi//5]
    b = table[0:2*lengi//5]
    c = table[0:3*lengi//5]
    d = table[0:4*lengi//5]

    reti = []
    reti.append(maxoftalbe(a))
    reti.append(maxoftalbe(b))
    reti.append(maxoftalbe(c))
    reti.append(maxoftalbe(d))

    csvaki.close()
    return reti


if __name__ == "__main__":

    myOut = open("out2", "r")
    myOutLines = myOut.readlines()
    myOut.close()
    # replace the \n with nothing
    myOutLines = [x.replace("\n", "") for x in myOutLines]
    print("name", " spark.app.name", " spark.executor.instances", "spark.executor.cores", " spark.executor.memory(GB)",
          " input.size (1000 lines)", " sumCpu (s  )", " maxHeap (MB  )", " maxNonHeap(MB  )", " sumRun(s  )",
          " sumjvmcpu(s  )", " maxdrheap(MB  )", " sumCpu (s  1/5)", " maxHeap (MB  1/5)", " maxNonHeap(MB  1/5)", " sumRun(s  1/5)",
          " sumjvmcpu(s  1/5)", " maxdrheap(MB  1/5)", " sumCpu (s  2/5)", " maxHeap (MB  2/5)", " maxNonHeap(MB  2/5)",
          " sumRun(s  2/5)", " sumjvmcpu(s  2/5)", " maxdrheap(MB  2/5)", " sumCpu (s  3/5)", " maxHeap (MB  3/5)",
          " maxNonHeap(MB  3/5)", " sumRun(s  3/5)", " sumjvmcpu(s  3/5)", " maxdrheap(MB  3/5)", " sumCpu (s  4/5)",
          " maxHeap (MB  4/5)", " maxNonHeap(MB  4/5)", " sumRun(s  4/5)", " sumjvmcpu(s  4/5)", " maxdrheap(MB  4/5)", sep=",")
    for name in myOutLines:
        sumCpu = 0
        sumRun = 0
        sumjvmcpu = 0
        maxHeap = 0
        maxNonHeap = 0
        areInstancesSet = False
        quortersdrheap = [0, 0, 0, 0]
        quortersCpu = [0, 0, 0, 0]
        quortersRun = [0, 0, 0, 0]
        quortersjvmcpu = [0, 0, 0, 0]
        quortersHeap = [0, 0, 0, 0]
        quortersNonHeap = [0, 0, 0, 0]
        # its sum of many files
        for i in range(15):
            # return true if a file exist
            if os.path.isfile("./cpu/"+name+"."+str(i)+".executor.cpuTime.csv"):

                sumCpu += getmax(name, "./cpu/", "."+str(i) +
                                 ".executor.cpuTime.csv")

                quortersCpu = [x + y for x, y in zip(quortersCpu, getQuorters(name, "./cpu/", "."+str(i) +
                                                                              ".executor.cpuTime.csv"))]
            elif not areInstancesSet:
                areInstancesSet = True
                temp = i,

            if os.path.isfile("./jvmcpu/"+name+"."+str(i)+".executor.jvmCpuTime.csv"):

                sumjvmcpu += getmax(name, "./jvmcpu/", "."+str(i) +
                                    ".executor.jvmCpuTime.csv")

                quortersjvmcpu = [x + y for x, y in zip(quortersjvmcpu, getQuorters(name, "./jvmcpu/", "."+str(i) +
                                                                                    ".executor.jvmCpuTime.csv"))]

            if os.path.isfile("./run/"+name+"."+str(i)+".executor.runTime.csv"):
                sumRun += getmax(name, "./run/", "."+str(i) +
                                 ".executor.runTime.csv")

                quortersRun = [x + y for x, y in zip(quortersRun, getQuorters(name, "./run/", "."+str(i) +
                                                                              ".executor.runTime.csv"))]

            # is max of many
            if os.path.isfile("./heap/"+name+"."+str(i)+".jvm.heap.used.csv"):
                maxHeap = max(maxHeap, getmax(name, "./heap/", "." +
                                              str(i)+".jvm.heap.used.csv"))
                quortersHeap = [max(x, y) for x, y in zip(quortersHeap, getQuorters(name, "./heap/", "."+str(i) +
                                                                                    ".jvm.heap.used.csv"))]

            if os.path.isfile("./nonheap/"+name+"."+str(i)+".jvm.non-heap.used.csv"):
                maxNonHeap = max(maxNonHeap, getmax(name, "./nonheap/", "." +
                                                    str(i)+".jvm.non-heap.used.csv"))

                quortersNonHeap = [max(x, y) for x, y in zip(quortersNonHeap, getQuorters(name, "./nonheap/", "."+str(i) +
                                                                                          ".jvm.non-heap.used.csv"))]

        # is actualy max of one
        maxdrheap = getmax(name, "./drheap/",
                           ".driver.BlockManager.memory.maxOnHeapMem_MB.csv")
        quortersdrheap = getQuorters(name, "./drheap/",
                                     ".driver.BlockManager.memory.maxOnHeapMem_MB.csv")

        myParams = json.load(open("./params/"+name+".inputParams"))
        myParams = dict(myParams)
        if temp[0] == 0:
            print(temp)
        myParams["spark.executor.instances"] = temp[0]
        # print elements of myParams from 0 to 10
        print(name,
              myParams["spark.app.name"],
              myParams["spark.executor.instances"],
              myParams["spark.executor.cores"],
              myParams["spark.executor.memory"][0],
              math.floor(int(myParams["input.size"])/1000),
              math.floor(sumCpu/1000000000),
              math.floor(maxHeap/1000000),
              math.floor(maxNonHeap/1000000),
              math.floor(sumRun/1000),
              math.floor(sumjvmcpu/1000000000),
              maxdrheap,
              math.floor(quortersCpu[0]/1000000000),
              math.floor(quortersHeap[0]/1000000),
              math.floor(quortersNonHeap[0]/1000000),
              math.floor(quortersRun[0]/1000),
              math.floor(quortersjvmcpu[0]/1000000000),
              math.floor(quortersdrheap[0]),
              math.floor(quortersCpu[1]/1000000000),
              math.floor(quortersHeap[1]/1000000),
              math.floor(quortersNonHeap[1]/1000000),
              math.floor(quortersRun[1]/1000),
              math.floor(quortersjvmcpu[1]/1000000000),
              math.floor(quortersdrheap[1]),
              math.floor(quortersCpu[2]/1000000000),
              math.floor(quortersHeap[2]/1000000),
              math.floor(quortersNonHeap[2]/1000000),
              math.floor(quortersRun[2]/1000),
              math.floor(quortersjvmcpu[2]/1000000000),
              math.floor(quortersdrheap[2]),
              math.floor(quortersCpu[3]/1000000000),
              math.floor(quortersHeap[3]/1000000),
              math.floor(quortersNonHeap[3]/1000000),
              math.floor(quortersRun[3]/1000),
              math.floor(quortersjvmcpu[3]/1000000000),
              math.floor(quortersdrheap[3]),
              sep=",")
