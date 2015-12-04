
from .. import analyze
import pyutil

class FalseDetectionCounter(analyze.BaseAnalyzer):

  def __init__(self):
    self.outputName = 'falsedetect'
    # this is a nested map that maps observer -> observee -> num false detect
    self.falseDetectionMap = {}
    self.revertFalseDetectionMap = {}

  def analyze(self, logLine, **kwargs):
    observer = kwargs['nid']
    if 'now Down' in logLine:
      tokens = logLine.split()
      observee = pyutil.ip2nid[tokens[8][1:]]
      if observer not in self.falseDetectionMap:
        self.falseDetectionMap[observer] = {}
      if observee not in self.falseDetectionMap[observer]:
        self.falseDetectionMap[observer][observee] = 0
      self.falseDetectionMap[observer][observee] += 1
      if observee not in self.revertFalseDetectionMap:
        self.revertFalseDetectionMap[observee] = {}
      if observer not in self.revertFalseDetectionMap[observee]:
        self.revertFalseDetectionMap[observee][observer] = 0
      self.revertFalseDetectionMap[observee][observer] += 1

  def analyzedResult(self):
    numFalseDetections = 0
    numFlappings = 0
    numsFalseObservers = []
    numsFalseObservees = []

    for observer in self.falseDetectionMap:
      numsFalseObservers.append(len(self.falseDetectionMap[observer]))
      for observee in self.falseDetectionMap[observer]:
        tmp = self.falseDetectionMap[observer][observee]
        numFalseDetections += tmp
        if tmp > 1:
          numFlappings += 1
    for observee in self.revertFalseDetectionMap:
      numsFalseObservees.append(len(self.revertFalseDetectionMap[observee]))

    return { 
      'num_false_detections' : str(numFalseDetections) + '\n', 
      'num_flappings' : str(numFlappings) + '\n', 
      'nums_false_observers' : '\n'.join(numsFalseObservers), 
      'nums_false_observees' : '\n'.join(numsFalseObservees)
    }

