
from .. import analyze
import pyutil

from collections import OrderedDict

class TSilenceExtractor(analyze.BaseAnalyzer):

  def __init__(self):
    allNides = pyutil.nid2ip.keys()
    self.numTsilence = 0.0
    self.tsilenceCount = {}
    self.tsilenceByTime = {}
    self.tsilenceAvgCount = {}
    self.tsilenceAvgByTime = {}

  def analyze(self, logLine, **kwargs):
    if ' t_silence ' in logLine:
      tokens = logLine.split()
      timestamp = analyze.extractTimestamp(tokens)
      self.numTsilence += 1

      tsilence = round(float(tokens[12]) / 1000, 2)
      if tsilence not in self.tsilenceCount:
        self.tsilenceCount[tsilence] = 0
      self.tsilenceCount[tsilence] += 1
      if timestamp not in self.tsilenceByTime:
        self.tsilenceByTime[timestamp] = []
      self.tsilenceByTime[timestamp].append(tsilence)

      tsilenceAvg = round(float(tokens[14]) / 1000, 2)
      if tsilenceAvg not in self.tsilenceAvgCount:
        self.tsilenceAvgCount[tsilenceAvg] = 0
      self.tsilenceAvgCount[tsilenceAvg] += 1
      if timestamp not in self.tsilenceAvgByTime:
        self.tsilenceAvgByTime[timestamp] = []
      self.tsilenceAvgByTime[timestamp].append(tsilenceAvg)

  def analyzedResult(self):

    tsilenceByTimeResult = ''
    for timestamp in sorted(self.tsilenceByTime.keys()):
      tsilenceThisSec = self.tsilenceByTime[timestamp]
      avgOfTsilence = sum(tsilenceThisSec) / len(tsilenceThisSec)
      tsilenceByTimeResult += '%d %f\n' % (timestamp, avgOfTsilence)

    tsilenceAvgByTimeResult = ''
    for timestamp in sorted(self.tsilenceAvgByTime.keys()):
      tsilenceAvgThisSec = self.tsilenceAvgByTime[timestamp]
      avgOfTsilenceAvg = sum(tsilenceAvgThisSec) / len(tsilenceAvgThisSec)
      tsilenceAvgByTimeResult += '%d %f\n' % (timestamp, avgOfTsilenceAvg)

    tsilenceCdf = ''
    cumulative = 0.0
    tsilenceStat = [ None ] * 5
    minVal = None
    maxVal = None
    average = 0
    averageT75 = 0
    cumulative75 = 0
    averageT90 = 0
    cumulative90 = 0
    averageT99 = 0
    cumulative99 = 0
    for tsilence in sorted(self.tsilenceCount.keys()):
      if not minVal:
        minVal = tsilence
      maxVal = tsilence
      thisTsilencePercent = self.tsilenceCount[tsilence] / self.numTsilence
      cumulative += thisTsilencePercent
      tsilenceCdf += '%f %f %f\n' % (tsilence, thisTsilencePercent, cumulative)
      if not tsilenceStat[1] and cumulative > 0.25:
        tsilenceStat[1] = tsilence
      elif not tsilenceStat[2] and cumulative > 0.5:
        tsilenceStat[2] = tsilence
      elif not tsilenceStat[3] and cumulative > 0.75:
        tsilenceStat[3] = tsilence
      weightAverage = tsilence * thisTsilencePercent
      average += weightAverage
      if cumulative > 0.75:
        averageT75 += weightAverage
        cumulative75 += thisTsilencePercent
      if cumulative > 0.90:
        averageT90 += weightAverage
        cumulative90 += thisTsilencePercent
      if cumulative > 0.99:
        averageT99 += weightAverage
        cumulative99 += thisTsilencePercent
    tsilenceStat[0] = minVal
    tsilenceStat[4] = maxVal
    avgTSilence = '%f %f %f %f\n' % (average, averageT75 / cumulative75, averageT90 / cumulative90, averageT99 / cumulative99)

    tsilenceAvgCdf = ''
    cumulative = 0.0
    tsilenceAvgStat = [ None ] * 5
    minVal = None
    maxVal = None
    average = 0
    #interestingTails = [ 75, 90, 99 ]
    #ails = { i : [0, 0] for i in interestingTails }
    averageT75 = 0
    cumulative75 = 0
    averageT90 = 0
    cumulative90 = 0
    averageT99 = 0
    cumulative99 = 0
    for tsilenceAvg in sorted(self.tsilenceAvgCount.keys()):
      if not minVal:
        minVal = tsilenceAvg
      maxVal = tsilenceAvg
      thisTsilenceAvgPercent = self.tsilenceAvgCount[tsilenceAvg] / self.numTsilence
      cumulative += thisTsilenceAvgPercent
      tsilenceAvgCdf += '%f %f %f\n' % (tsilenceAvg, thisTsilenceAvgPercent, cumulative)
      if not tsilenceAvgStat[1] and cumulative > 0.25:
        tsilenceAvgStat[1] = tsilenceAvg
      elif not tsilenceAvgStat[2] and cumulative > 0.5:
        tsilenceAvgStat[2] = tsilenceAvg
      elif not tsilenceAvgStat[3] and cumulative > 0.75:
        tsilenceAvgStat[3] = tsilenceAvg
      weightAverage = tsilenceAvg * thisTsilenceAvgPercent
      average += weightAverage
      if cumulative > 0.75:
        averageT75 += weightAverage
        cumulative75 += thisTsilenceAvgPercent
      if cumulative > 0.90:
        averageT90 += weightAverage
        cumulative90 += thisTsilenceAvgPercent
      if cumulative > 0.99:
        averageT99 += weightAverage
        cumulative99 += thisTsilenceAvgPercent
    tsilenceAvgStat[0] = minVal
    tsilenceAvgStat[4] = maxVal
    avgTSilenceAvg = '%f %f %f %f\n' % (average, averageT75 / cumulative75, averageT90 / cumulative90, averageT99 / cumulative99)

    return { 
      't_silence_stat' : ' '.join(map(str, tsilenceStat)) + '\n',
      't_silence_cdf' : tsilenceCdf,
      't_silence_over_time' : tsilenceAvgByTimeResult,
      'avg_t_silence' : avgTSilence,
      't_silence_avg_stat' : ' '.join(map(str, tsilenceStat)) + '\n',
      't_silence_avg_cdf' : tsilenceAvgCdf,
      't_silence_avg_over_time' : tsilenceAvgByTimeResult,
      'avg_t_silence_avg' : avgTSilenceAvg,
    }

