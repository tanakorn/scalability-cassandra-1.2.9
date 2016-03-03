
from .. import analyze
import pyutil

class ExecTimeAnalyzer(analyze.BaseAnalyzer):

  def __init__(self):
    self.execTimes = []
    self.calcRangeTimes = []

  def analyze(self, logLine, **kwargs):
    if ' executes ' in logLine:
      tokens = logLine.split()
      execTime = float(tokens[9]) / 1000
      if execTime > 0.1:
        self.execTimes.append(execTime)
    elif 'calculatePendingRanges' in logLine:
      tokens = logLine.split()
      calcRangeTime = float(tokens[7]) / 1000
      self.calcRangeTimes.append(calcRangeTime)

  def analyzedResult(self):
    result = { } 

    statStr = analyze.calcStatStr(self.execTimes)
    result['exec_time_stat'] = statStr
    cdfStr = analyze.calcCdfStr(self.execTimes, 2)
    result['exec_time_cdf'] = cdfStr

    statStr = analyze.calcStatStr(self.calcRangeTimes)
    result['calc_range_time_stat'] = statStr
    cdfStr = analyze.calcCdfStr(self.calcRangeTimes, 2)
    result['calc_range_time_cdf'] = cdfStr

    return result
  
