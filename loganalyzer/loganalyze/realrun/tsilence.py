
from .. import analyze
import pyutil

from collections import OrderedDict

class TSilenceExtractor(analyze.BaseAnalyzer):

  def __init__(self):
    allNides = pyutil.nid2ip.keys()
    self.numTsilence = 0.0
    self.tsilenceCount = {}
    self.tsilenceAvgCount = {}
    self.tsilenceAvgByTime = { }

  def analyze(self, logLine, **kwargs):
    if ' t_silence ' in logLine:
      tokens = logLine.split()
      timestamp = analyze.extractTimestamp(tokens)
      self.numTsilence += 1
      tsilence = round(float(tokens[12]) / 1000, 2)
      if tsilence not in self.tsilenceCount:
        self.tsilenceCount[tsilence] = 0
      self.tsilenceCount[tsilence] += 1
      tsilenceAvg = round(float(tokens[14]) / 1000, 2)
      if tsilenceAvg not in self.tsilenceAvgCount:
        self.tsilenceAvgCount[tsilenceAvg] = 0
      self.tsilenceAvgCount[tsilenceAvg] += 1
      if timestamp not in self.tsilenceAvgByTime:
        self.tsilenceAvgByTime[timestamp] = []
      self.tsilenceAvgByTime[timestamp].append(tsilenceAvg)

  def analyzedResult(self):
    tsilenceAvgByTime = OrderedDict(sorted(self.tsilenceAvgByTime.items(), key=lambda x: x[0]))
    tsilenceAvgByTimeResult = ''
    for timestamp in tsilenceAvgByTime:
      tsilenceAvgThisSec = tsilenceAvgByTime[timestamp]
      avgOfTsilenceAvg = sum(tsilenceAvgThisSec) / len(tsilenceAvgThisSec)
      tsilenceAvgByTimeResult += '%d %f\n' % (timestamp, avgOfTsilenceAvg)

    tsilenceCdf = ''
    cumulative = 0.0
    tsilenceStat = [ None ] * 5
    minVal = None
    maxVal = None
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
    tsilenceStat[0] = minVal
    tsilenceStat[4] = maxVal

    tsilenceAvgCdf = ''
    cumulative = 0.0
    for tsilenceAvg in sorted(self.tsilenceAvgCount.keys()):
      thisTsilenceAvgPercent = self.tsilenceAvgCount[tsilenceAvg] / self.numTsilence
      cumulative += thisTsilenceAvgPercent
      tsilenceAvgCdf += '%f %f %f\n' % (tsilenceAvg, thisTsilenceAvgPercent, cumulative)

    return { 
      't_silence_stat' : ' '.join(map(str, tsilenceStat)) + '\n',
      't_silence_cdf' : tsilenceCdf,
      't_silence_avg_cdf' : tsilenceAvgCdf,
      't_silence_avg_over_time' : tsilenceAvgByTimeResult
    }

