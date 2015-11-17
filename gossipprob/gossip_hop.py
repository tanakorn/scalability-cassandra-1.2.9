#!/usr/bin/env python

import sys
import math

def infectionProb(numNode, numInfect):
  return 1 - math.pow(1 - 1 / (float(numNode) - 1), float(numInfect))

def numIncreasingInfect(numNode, numInfect):
  return (numNode - numInfect) * infectionProb(numNode, numInfect)

def main():
  if len(sys.argv) == 1:
    print 'usage: infection.py <num_node>'
    exit(1)
  numNode = int(sys.argv[1])
  if numNode <= 1:
    print 'Number of nodes must be greater than 1'
    exit(2)
  numInfect = 1.0
  time = 0
  infectRecord = [ numInfect ]
  while round(numInfect) < numNode:
    increaseInfect = [ 0.0 ] * len(infectRecord)
    for i, inf1 in enumerate(infectRecord):
      increaseInfect[i] = infectionProb(numNode, inf1)
      increaseInfect[i] *= (1.0 - infectionProb(numNode, sum(infectRecord[:i])))
      increaseInfect[i] *= (numNode - sum(infectRecord))
    infectRecord.append(0.0)
    for i in range(len(increaseInfect)):
      infectRecord[i + 1] += increaseInfect[i]
    numInfect += numIncreasingInfect(numNode, numInfect)
    time += 1
    #print time, numInfect, sum(infectRecord), infectRecord

  cdfProb = 0.0
  for i, inf in enumerate(infectRecord):
    prob = inf / numNode
    cdfProb += prob
    print i + 1, inf, prob, cdfProb

if __name__ == '__main__':
  main()

