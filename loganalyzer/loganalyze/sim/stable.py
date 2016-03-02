
from .. import analyze
import pyutil

class StableAnalyzer(analyze.BaseAnalyzer):

  def __init__(self):
    # ringInfo is a list, [ startTime, stableTime ] stableTime = None means not stable
    self.clusterRingInfo = [ None, None ]

  def analyze(self, logLine, **kwargs):
    if ' stable ' in logLine:
      tokens = logLine.split()
      timestamp = analyze.extractTimestamp(tokens)
      isStable = True if tokens[9] == 'yes' else False
      if not self.clusterRingInfo[0]:
        self.clusterRingInfo[0] = timestamp
      if isStable:
        if not self.clusterRingInfo[1]:
          self.clusterRingInfo[1] = timestamp
      else:
        self.clusterRingInfo[1] = None

  def analyzedResult(self):
    
    stableTime = -1 if not self.clusterRingInfo[1] else self.clusterRingInfo[1] - self.clusterRingInfo[0]

    return { 'stable_time' : str(stableTime) + '\n' }
  
