#!/usr/bin/env python

import sys
import math

def infectionProb(numNode, numInfect):
  return 1 - math.pow(1 - 1 / (float(numNode) - 1), float(numInfect))

def numIncreasingInfect(numNode, numInfect):
  return (numNode - numInfect) * infectionProb(numNode, numInfect)

def main():
  if len(sys.argv) < 3:
    print 'usage: infection.py <num_node> <exec_time_record>'
    exit(1)
  numNode = int(sys.argv[1])
  execRecord = open(sys.argv[2])
  rawData = execRecord.readlines()
  execTime = [ float(i[1]) / 1000 for i in map(lambda x: x.split(), rawData) ]
  if numNode <= 1:
    print 'Number of nodes must be greater than 1'
    exit(2)
  numInfect = 1.0
  time = 0
  infectRecord = [ numInfect ]
  infectRecordRoundHop = [ [ numInfect ] ]
  while round(numInfect, 2) < numNode:
    increaseInfect = [ 0.0 ] * len(infectRecord)
    for i, inf1 in enumerate(infectRecord):
      increaseInfect[i] = infectionProb(numNode, inf1)
      increaseInfect[i] *= (1.0 - infectionProb(numNode, sum(infectRecord[:i])))
      increaseInfect[i] *= (numNode - sum(infectRecord))
    infectRecord.append(0.0)
    for i in range(len(increaseInfect)):
      infectRecord[i + 1] += increaseInfect[i]
    increaseInfect.insert(0, 0.0)
    infectRecordRoundHop.append(increaseInfect)
    numInfect += numIncreasingInfect(numNode, numInfect)
    time += 1

  medExecTime = execTime[numNode / 2]
  realTimeMap = { }
  for i, r in enumerate(infectRecordRoundHop):
    for j, h in enumerate(r):
      realTime = round(i + j * medExecTime, 2)
      if not realTime in realTimeMap:
        realTimeMap[realTime] = 0.0
      realTimeMap[realTime] += h
  cdfProb = 0.0
  for i in sorted(realTimeMap.keys()):
    inf = realTimeMap[i]
    prob = inf / numNode
    cdfProb += prob
    print i, inf, prob, cdfProb

if __name__ == '__main__':
  main()

