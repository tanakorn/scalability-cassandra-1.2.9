
from .. import analyze
import pyutil

class MessageCounter(analyze.BaseAnalyzer):

  def __init__(self):
    self.synCount = 0
    self.ackCount = 0
    self.ack2Count = 0
    self.allCount = 0
    #self.maxQueueSize = { i : 0 for i in pyutil.ip2nid.keys() } 
    self.maxQueueSize = 0

  def analyze(self, logLine, **kwargs):
    if ' doVerb ' in logLine:
      self.allCount += 1
      tokens = logLine.split()
      if tokens[9] == 'syn':
        self.synCount += 1
      elif tokens[9] == 'ack':
        self.ackCount += 1
      elif tokens[9] == 'ack2':
        self.ack2Count += 1
    if ' queue ' in logLine:
      tokens = logLine.split()
      #receiverIp = tokens[6][1:]
      queueSize = int(tokens[15])
      if queueSize > self.maxQueueSize:
        self.maxQueueSize = queueSize

  def analyzedResult(self):
    msgCount = '%d %d %d %d\n' % tuple(map(lambda x: x / pyutil.num_node, (self.synCount, self.ackCount, self.ack2Count, self.allCount)))
    return { 
        'avg_msg_count' : msgCount,
        'max_queue_size' : '%d\n' % self.maxQueueSize
    }
  
