from .. import analyze

from ..realrun import tsilence
import pyutil

class TSilenceExtractor(tsilence.TSilenceExtractor):

  def analyze(self, logLine, **kwargs):
    if ' t_silence ' in logLine:
      tokens = logLine.split()
      timestamp = analyze.extractTimestamp(tokens)
      timeData = tokens[9][:-1].split(',')
      timeData = map(lambda x: x.split(':'), timeData)
      tsilence = map(lambda x: float(x[0]) / 1000, timeData)
      tsilenceAvg = map(lambda x: float(x[1]) / 1000, timeData)
      self.tsilence += tsilence
      self.tsilenceAvg += tsilenceAvg
      if timestamp not in self.tsilenceAvgByTime:
        self.tsilenceAvgByTime[timestamp] = []
      self.tsilenceAvgByTime[timestamp] += tsilenceAvg

