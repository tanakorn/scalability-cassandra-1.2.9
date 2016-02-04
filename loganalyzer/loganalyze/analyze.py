
from datetime import *
from time import mktime
import math

class BaseAnalyzer(object):

  def __init__(self):
    pass

  def analyze(self, logLine, **kwargs):
    pass

  def analyzedResult(self):
    return {}

  def writeOutput(self, outputDir, suffix=''):
    suffix = '' if not suffix else '_' + suffix
    result = self.analyzedResult()
    for fileName in result:
      output = open('%s/%s%s' % (outputDir, fileName, suffix), 'w')
      output.write(result[fileName])
      output.close()

extractedTimeCache = { }
def extractTime(logLine):
  global extractedTimeCache
  if type(logLine) == str:
    tokens = logLine.split()
  elif type(logLine) == list:
    tokens = logLine
  timeStr = (tokens[2] + ' ' + tokens[3]).split(',')[0]
  if timeStr in extractedTimeCache:
    return extractedTimeCache[timeStr]
  timeObj = datetime.strptime(timeStr, '%Y-%m-%d %X')
  extractedTimeCache[timeStr] = timeObj
  return timeObj

extractedTimestampCache = { }
def extractTimestamp(logLine):
  global extractedTimeCache
  global extractedTimestampCache
  if type(logLine) == str:
    tokens = logLine.split()
  elif type(logLine) == list:
    tokens = logLine
  timeStr = (tokens[2] + ' ' + tokens[3]).split(',')[0]
  if timeStr in extractedTimestampCache:
    return extractedTimestampCache[timeStr]
  if timeStr in extractedTimeCache:
    timeObj = extractedTimeCache[timeStr]
  else:
    timeObj = datetime.strptime(timeStr, '%Y-%m-%d %X')  
    extractedTimeCache[timeStr] = timeObj
  timestamp =  mktime(timeObj.timetuple())
  extractedTimestampCache[timeStr] = timestamp
  return timestamp

def calcStat(data):
  dataSize = len(data)
  if not dataSize:
    return 0, 0, 0, 0, 0
  sortedData = sorted(data)
  lqPos = dataSize / 4
  medPos = dataSize / 2
  uqPos = dataSize * 3 / 4
  return sortedData[0], sortedData[lqPos], sortedData[medPos], sortedData[uqPos], sortedData[-1]

def calcAverage(data):
  avg = sum(data) / len(data)
  sd = map(lambda x: math.pow(x - avg, 2), data)
  sd = sum(sd) / len(sd)
  sd = math.pow(sd, 0.5)
  return avg, sd

def calcCdf(data, ndigit):
  dataSize = len(data)
  valueCount = {}
  for i in data:
    value = round(i, ndigit)
    if value not in valueCount:
      valueCount[value] = 0
    valueCount[value] += 1
  cumulative = 0.0
  cdf = {}
  for i in sorted(valueCount.keys()):
    cumulative += valueCount[i]
    cdf[i] = cumulative / dataSize
  return cdf
