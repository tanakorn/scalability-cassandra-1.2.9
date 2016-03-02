from .. import analyze

from ..real import tsilence
import pyutil

class TSilenceExtractor(tsilence.TSilenceExtractor):

  def analyze(self, logLine, **kwargs):
    if ' t_silence ' in logLine:
      tokens = logLine.split()
      timestamp = analyze.extractTimestamp(tokens)
      timeData = tokens[9][:-1].split(',')
      timeData = map(lambda x: x.split(':'), timeData)
      tsilence = map(lambda x: round(float(x[0]) / 1000, 2), timeData)
      tsilenceAvg = map(lambda x: round(float(x[1]) / 1000, 2), timeData)

      self.numTsilence += len(tsilence)
      for time in tsilence:
        if time not in self.tsilenceCount:
          self.tsilenceCount[time] = 0
        self.tsilenceCount[time] +=1
      for time in tsilenceAvg:
        if time not in self.tsilenceAvgCount:
          self.tsilenceAvgCount[time] = 0
        self.tsilenceAvgCount[time] +=1

      if timestamp not in self.tsilenceAvgByTime:
        self.tsilenceAvgByTime[timestamp] = []
      self.tsilenceAvgByTime[timestamp] += tsilenceAvg

