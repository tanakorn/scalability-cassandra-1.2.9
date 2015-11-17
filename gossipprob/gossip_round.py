#!/usr/bin/env python

import sys
import math

def numIncreasingInfect(numNode, numInfect):
  return (numNode - numInfect) * infectionProb(numNode, numInfect)

def numNextInfect(numNode, numInfect):
  return numInfect + numIncreasingInfect(numNode, numInfect)

def infectionProb(numNode, numInfect):
  return 1 - math.pow(1 - 1 / (float(numNode) - 1), float(numInfect))

def mistakeProb(numNode, numInfect):
  upperBound = 1 - math.pow(numInfect / numNode, numNode)
  return upperBound

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
  while round(numInfect, 2) < numNode:
    numInfect = numNextInfect(numNode, numInfect)
    time += 1
    print time, numInfect, numInfect / numNode, 1 - mistakeProb(numNode, numInfect)

if __name__ == '__main__':
  main()

