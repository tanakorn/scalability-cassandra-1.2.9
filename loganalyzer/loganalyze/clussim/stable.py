
from .. import analyze
import pyutil

class StableAnalyzer(analyze.BaseAnalyzer):

  def __init__(self):
    # ringInfo is a list, [ startTime, stableTime ] stableTime = None means not stable
    self.ringInfo = {}

  def analyze(self, logLine, **kwargs):
    if ' ringinfo ' in logLine:
      tokens = logLine.split()
      timestamp = analyze.extractTimestamp(tokens)
      address = tokens[9][1:]
      nid = pyutil.ip2nid[address]
      if nid not in self.ringInfo:
        self.ringInfo[nid] = [ timestamp, None ]
      else:
        memberNode = int(tokens[17][0:-1])
        deadNode = int(tokens[21])
        if memberNode < pyutil.num_nodes or deadNode != 0:
          self.ringInfo[nid][1] = None
        elif not self.ringInfo[nid][1]:
          self.ringInfo[nid][1] = timestamp

  def analyzedResult(self):

    clusterStartTime = None
    clusterStableTime = 0
    for nid in self.ringInfo:
      ringInfo = self.ringInfo[nid]
      if not clusterStartTime or clusterStartTime > ringInfo[0]:
        clusterStartTime = ringInfo[0]
      if not ringInfo[1]:
        break
      elif clusterStableTime < ringInfo[1]:
        clusterStableTime = ringInfo[1]

    stableTime = 0
    if not clusterStableTime:
      stableTime = -1
    else:
      stableTime = clusterStableTime - clusterStartTime

    return { 'stable_time' : str(stableTime) + '\n' }
  
