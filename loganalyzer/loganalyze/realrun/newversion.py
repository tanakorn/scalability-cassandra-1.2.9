
from .. import analyze
import pyutil

class NewVersionCounter(analyze.BaseAnalyzer):

  def __init__(self):
    self.gossipType = ('gossip_ack', 'gossip_ack2', 'gossip_all')
    self.newVersion = { i : { 'boot' : [], 'normal' : [] } for i in self.gossipType }

  def analyze(self, logLine, **kwargs):
    if ' apply ' in logLine:
      tokens = logLine.split()
      execType = tokens[9]
      self.newVersion[execType]['boot'].append(tokens[11])
      self.newVersion[execType]['normal'].append(tokens[13])

  def analyzedResult(self):
    result = { }
    for execType in self.gossipType:
      result['new_boot_' + execType] = '\n'.join(self.newVersion[execType]['boot']) + '\n'
      result['new_normal_' + execType] = '\n'.join(self.newVersion[execType]['normal']) + '\n'
    return result 
