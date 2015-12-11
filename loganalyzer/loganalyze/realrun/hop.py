
from .. import analyze
import pyutil

class HopCounter(analyze.BaseAnalyzer):

  def __init__(self):
    self.hops = []

  def analyze(self, logLine, **kwargs):
    if ' hop ' in logLine:
      tokens = logLine.split()
      self.hops.append(int(tokens[10]))

  def analyzedResult(self):
    minVal, lqVal, medVal, uqVal, maxVal = analyze.calcStat(self.hops)
    mean = sum(self.hops) / len(self.hops)
    result = 'mean=%f min=%f lq=%f med=%f uq=%f max=%f\n' % (mean, minVal, lqVal, medVal, uqVal, maxVal)
    return { 'hop' : result }
  
