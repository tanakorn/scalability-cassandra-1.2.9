
from .. import analyze
import pyutil

from collections import OrderedDict

class TSilenceExtractor(analyze.BaseAnalyzer):

  def __init__(self):
    allNides = pyutil.nid2ip.keys()
    self.tsilence = []
    self.tsilenceAvg = []
    self.tsilenceAvgByTime = { }

  def analyze(self, logLine, **kwargs):
    if ' t_silence ' in logLine:
      tokens = logLine.split()
      timestamp = analyze.extractTimestamp(tokens)
      self.tsilence.append(float(tokens[12]) / 1000)
      tsilenceAvg = float(tokens[14]) / 1000
      self.tsilenceAvg.append(tsilenceAvg)
      #observer = pyutil.ip2nid[tokens[7][1:]]
      #observee = pyutil.ip2nid[tokens[10][1:]]
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

    return { 
      't_silence' : '\n'.join(map(str, self.tsilence)) + '\n',
      't_silence_avg' : '\n'.join(map(str, self.tsilenceAvg)) + '\n',
      't_silence_avg_over_time' : tsilenceAvgByTimeResult
    }

