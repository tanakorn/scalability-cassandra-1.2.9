
from .. import analyze
import pyutil

class MessageCounter(analyze.BaseAnalyzer):

  def __init__(self):
    self.seedSynCount = 0
    self.seedAckCount = 0
    self.seedAck2Count = 0
    self.normSynCount = 0
    self.normAckCount = 0
    self.normAck2Count = 0
    self.synCount = 0
    self.ackCount = 0
    self.ack2Count = 0
    self.allCount = 0
    self.maxQueueSize = 0

  def analyze(self, logLine, **kwargs):
    if ' doVerb ' in logLine:
      self.allCount += 1
      tokens = logLine.split()
      ip = tokens[7][1:]
      if tokens[9] == 'syn':
        self.synCount += 1
        if ip == '127.0.0.1':
          self.seedSynCount += 1
        else:
          self.normSynCount += 1
      elif tokens[9] == 'ack':
        self.ackCount += 1
        if ip == '127.0.0.1':
          self.seedAckCount += 1
        else:
          self.normAckCount += 1
      elif tokens[9] == 'ack2':
        self.ack2Count += 1
        if ip == '127.0.0.1':
          self.seedAck2Count += 1
        else:
          self.normAck2Count += 1
    if ' queue ' in logLine:
      tokens = logLine.split()
      queueSize = int(tokens[12])
      if queueSize > self.maxQueueSize:
        self.maxQueueSize = queueSize

  def analyzedResult(self):
    msgCount = 'avg_syn=%d avg_ack=%d avg_ack2=%d avg_all=%d\n' \
        % tuple(map(lambda x: x / pyutil.num_node, \
            (self.synCount, self.ackCount, self.ack2Count, self.allCount)))
    msgCount += 'seed_syn=%d seed_ack=%d seed_ack2=%d\n' \
        % (self.seedSynCount, self.seedAckCount, self.seedAck2Count)
    msgCount += 'avg_oth_syn=%d avg_oth_ack=%d avg_oth_ack2=%d\n' \
        % tuple(map(lambda x: x / (pyutil.num_node - 1), \
            (self.normSynCount, self.normAckCount, self.normAck2Count)))
    return { 
        'avg_msg_count' : msgCount,
        'max_queue_size' : '%d\n' % (self.maxQueueSize / pyutil.num_node)
    }
  
