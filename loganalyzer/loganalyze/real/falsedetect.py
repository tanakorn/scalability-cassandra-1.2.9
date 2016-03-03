
from .. import analyze
import pyutil

from collections import OrderedDict

class FalseDetectionCounter(analyze.BaseAnalyzer):

  def __init__(self):
    # this is a nested map that maps observer -> observee -> num false detect
    allNids = pyutil.nid2ip.keys()
    self.falseMap = { observer : { observee : [] for observee in allNids } for observer in allNids }
    self.revertFalseMap = { observee : { observer : [] for observer in allNids } for observee in allNids }
    self.decomTime = None

  def analyze(self, logLine, **kwargs):
    observer = kwargs['nid']
    tokens = logLine.split()
    downKeyword = 'now DOWN'
    decomKeyword = 'Decommission operation starts'
    if logLine[-len(downKeyword):] == downKeyword:
      timestamp = int(tokens[4])
      observee = pyutil.ip2nid[tokens[7][1:]]
      self.falseMap[observer][observee].append(timestamp)
      self.revertFalseMap[observee][observer].append(timestamp)
    elif logLine[-len(decomKeyword):] == decomKeyword:
      self.decomTime = int(tokens[6])

  def analyzedResult(self):
    decomIp = '192.168.%d.1' % (pyutil.num_node / 8)
    decomId = pyutil.ip2nid[decomIp]
    numFalse = 0
    for observer in self.falseMap:
      for observee in self.falseMap[observer]:
        if observee != decomId:
          for timestamp in self.falseMap[observer][observee]:
            if self.decomTime < timestamp:
              numFalse += 1

    return {
      'num_false' : '%d\n' % numFalse,
    }

    '''
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
      if avgFalseInSec:
        allNumAccFalse = [ 0 ] * (pyutil.num_nodes - len(accFalse))
        allNumAccFalse.extend(map(len, accFalse.values()))
        minVal, lqVal, medVal, uqVal, maxVal = analyze.calcStat(allNumAccFalse)
        avgAccFalseResult += '%d mean=%d min=%d lq=%d med=%d uq=%d max=%d\n' % (sec, avgFalseInSec, minVal, lqVal, medVal, uqVal, maxVal)
      else:
        avgAccFalseResult += '%d mean=%d\n' % (sec, avgFalseInSec)

      for observee in deadDeclareForNodeInSec[sec]:
        if observee not in accDeadDeclare:
          accDeadDeclare[observee] = set()
        accDeadDeclare[observee].update(deadDeclareForNodeInSec[sec][observee])
      totalDeadDeclareInSec = 0
      for observee in accDeadDeclare:
        totalDeadDeclareInSec += len(accDeadDeclare[observee])
      avgDeadDeclare = totalDeadDeclareInSec / pyutil.num_nodes
      if avgDeadDeclare:
        allNumDeadDeclare = [ 0 ] * (pyutil.num_nodes - len(accDeadDeclare))
        allNumDeadDeclare.extend(map(len, accDeadDeclare.values()))
        minVal, lqVal, medVal, uqVal, maxVal = analyze.calcStat(allNumDeadDeclare)
        avgAccDeadDeclareResult += '%d mean=%d min=%d lq=%d med=%d uq=%d max=%d\n' % (sec, avgDeadDeclare, minVal, lqVal, medVal, uqVal, maxVal)
      else:
        avgAccDeadDeclareResult += '%d mean=%d\n' % (sec, avgDeadDeclare)

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
    '''

