
from .. import analyze
import pyutil

from collections import OrderedDict

class FalseDetectionCounter(analyze.BaseAnalyzer):

  def __init__(self):
    # this is a nested map that maps observer -> observee -> num false detect
    allNids = pyutil.nid2ip.keys()
    self.falseMap = { observer : { observee : [] for observee in allNids } for observer in allNids }
    self.revertFalseMap = { observee : { observer : [] for observer in allNids } for observee in allNids }
    self.startTimestamp = None
    self.endTimestamp = None

  def analyze(self, logLine, **kwargs):
    observer = kwargs['nid']
    tokens = logLine.split()
    timestamp = analyze.extractTimestamp(tokens)
    if not self.startTimestamp or timestamp < self.startTimestamp:
      self.startTimestamp = timestamp
    if not self.endTimestamp or timestamp > self.endTimestamp:
      self.endTimestamp = timestamp
    if 'now DOWN' in logLine:
      observee = pyutil.ip2nid[tokens[8][1:]]
      self.falseMap[observer][observee].append(timestamp)
      self.revertFalseMap[observee][observer].append(timestamp)

  def analyzedResult(self):
    numFalse = 0
    numFlappings = 0
    numsDeadObservees = []
    numsWrongObservers = []
    timeRange = range(int(self.startTimestamp), int(self.endTimestamp) + 1)
    numFalseInSec = { i : 0 for i in timeRange }
    # timestamp -> observer -> set of observee
    falseInNodeInSec = { i : {} for i in timeRange }
    deadDeclareForNodeInSec = { i : {} for i in timeRange }

    for observer in self.falseMap:
      thisNumDeadObservees = 0
      for observee in self.falseMap[observer]:
        tmp = self.falseMap[observer][observee]
        thisNumFalse = len(tmp)
        numFalse += thisNumFalse
        if thisNumFalse > 0:
          thisNumDeadObservees +=1
        if thisNumFalse > 1:
          numFlappings += 1
        for sec in tmp:
          numFalseInSec[sec] += 1
          if observer not in falseInNodeInSec[sec]:
            falseInNodeInSec[sec][observer] = set()
          falseInNodeInSec[sec][observer].add(observee)
      numsDeadObservees.append(thisNumDeadObservees)
    for observee in self.revertFalseMap:
      thisNumWrongObservers = 0
      for observer in self.revertFalseMap:
        tmp = self.revertFalseMap[observee][observer]
        thisNumFalse = len(tmp)
        if thisNumFalse > 0:
          thisNumWrongObservers += 1
        for sec in tmp:
          if observee not in deadDeclareForNodeInSec:
            deadDeclareForNodeInSec[sec][observee] = set()
          deadDeclareForNodeInSec[sec][observee].add(observer)
      numsWrongObservers.append(thisNumWrongObservers)

    numFalseInSec = OrderedDict(sorted(numFalseInSec.items(), key=lambda x: x[0]))
    numFalseInSecResult = reduce(lambda x, y: x + ('%d %d\n' % y), numFalseInSec.items(), '')
    falseInNodeInSec = OrderedDict(sorted(falseInNodeInSec.items(), key=lambda x: x[0]))
    deadDeclareForNodeInSec = OrderedDict(sorted(deadDeclareForNodeInSec.items(), key=lambda x: x[0]))
    accFalse = { }
    accFalseObserversResult = ''
    avgAccFalseResult = ''
    accDeadObservees = set()
    accDeadDeclare = { }
    accDeadObserveesResult = ''
    avgAccDeadDeclareResult = ''
    for sec in timeRange:
      for observer in falseInNodeInSec[sec]:
        if observer not in accFalse:
          accFalse[observer] = set()
        accFalse[observer].update(falseInNodeInSec[sec][observer])
        accDeadObservees.update(falseInNodeInSec[sec][observer])
      accFalseObserversResult += '%d %d\n' % (sec, len(accFalse))
      accDeadObserveesResult += '%d %d\n' % (sec, len(accDeadObservees))
      totalAccFalseInSec = 0
      for observer in accFalse:
        totalAccFalseInSec += len(accFalse[observer])
      avgFalseInSec = totalAccFalseInSec / pyutil.num_nodes
      avgAccFalseResult += '%d %d\n' % (sec, avgFalseInSec)

      for observee in deadDeclareForNodeInSec[sec]:
        if observee not in accDeadDeclare:
          accDeadDeclare[observee] = set()
        accDeadDeclare[observee].update(deadDeclareForNodeInSec[sec][observee])
      totalDeadDeclareInSec = 0
      for observee in accDeadDeclare:
        totalDeadDeclareInSec += len(accDeadDeclare[observee])
      avgDeadDeclare = totalDeadDeclareInSec / pyutil.num_nodes
      avgAccDeadDeclareResult += '%d %d\n' % (sec, avgDeadDeclare)

    return { 
      'num_false' : str(numFalse) + '\n', 
      'num_flappings' : str(numFlappings) + '\n', 
      'nums_dead_observees' : '\n'.join(map(str, numsDeadObservees)) + '\n', 
      'nums_wrong_observers' : '\n'.join(map(str, numsWrongObservers)) + '\n',
      'num_false_over_time' : numFalseInSecResult,
      'acc_false_observers' : accFalseObserversResult,
      'avg_acc_false' : avgAccFalseResult,
      'acc_dead_observees' : accDeadObserveesResult,
      'avg_acc_dead_declare' : avgAccDeadDeclareResult,
    }

