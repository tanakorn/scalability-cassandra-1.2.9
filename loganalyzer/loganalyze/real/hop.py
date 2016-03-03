
from .. import analyze
import pyutil

class HopCounter(analyze.BaseAnalyzer):

  def __init__(self):
    self.numHop = 0.0
    self.hopCount = {}

  def analyze(self, logLine, **kwargs):
    if ' hop ' in logLine:
      tokens = logLine.split()
      hop = int(tokens[10])
      if hop not in self.hopCount:
        self.hopCount[hop] = 0
      self.hopCount[hop] += 1
      self.numHop += 1

  def analyzedResult(self):

    hopCdf = ''
    cumulative = 0.0
    hopStat = [ None ] * 5
    minVal = None
    maxVal = None
    for hop in sorted(self.hopCount.keys()):
      if not minVal:
        minVal = hop
      maxVal = hop
      thisHopPercent = self.hopCount[hop] / self.numHop
      cumulative += thisHopPercent
      hopCdf += '%f %f %f\n' % (hop, thisHopPercent, cumulative)
      if not hopStat[1] and cumulative > 0.25:
        hopStat[1] = hop
      elif not hopStat[2] and cumulative > 0.5:
        hopStat[2] = hop
      elif not hopStat[3] and cumulative > 0.75:
        hopStat[3] = hop
    hopStat[0] = minVal
    hopStat[4] = maxVal

    return { 
      'hop_stat' : ' '.join(map(str, hopStat)) + '\n',
      'hop_cdf' : hopCdf,
    }
  
