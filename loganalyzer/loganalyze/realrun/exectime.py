
from .. import analyze
import pyutil

class ExecTimeAnalyzer(analyze.BaseAnalyzer):

  def __init__(self):
    self.execTime = { 'gossip_ack' : [], 'gossip_ack2' : [], 'gossip_all' : [] }

  def analyze(self, logLine, **kwargs):
    if ' executes ' in logLine:
      tokens = logLine.split()
      #observerIp = tokens[7][1:]
      #observer = pyutil.ip2nid[observerIp]
      execTime = float(tokens[11]) / 1000
      execType = tokens[9]
      self.execTime[execType].append(execTime)

  def analyzedResult(self):
    result = { } 
    for execType in self.execTime:
      #minVal, lqVal, medVal, uqVal, maxVal = analyze.calcStat(self.execTime[execType])
      #mean = sum(self.execTime[execType]) / len(self.execTime[execType])
      #result += '%s mean=%f min=%f lq=%f med=%f uq=%f max=%f\n' % (execType, mean, minVal, lqVal, medVal, uqVal, maxVal)
      result['exec_time_' + execType] = '\n'.join(map(str, self.execTime[execType])) + '\n'
    return result
  
