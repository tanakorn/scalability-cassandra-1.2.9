
from .. import analyze
import pyutil

class ExecTimeAnalyzer(analyze.BaseAnalyzer):

  def __init__(self):
    self.execTime = {}
    self.execTime2 = []

  def analyze(self, logLine, **kwargs):
    if ' executes2 ' in logLine:
      tokens = logLine.split()
      time = int(tokens[11])
      realUpdate = int(tokens[20])
      currentVersion = int(tokens[22])
      if currentVersion not in self.execTime:
        self.execTime[currentVersion] = {}
      if realUpdate not in self.execTime[currentVersion]:
        self.execTime[currentVersion][realUpdate] = []
      self.execTime[currentVersion][realUpdate].append(time)
      self.execTime2.append(time)

  def analyzedResult(self):

    resultTable = ''
    for currentVersion in sorted(self.execTime):
      for realUpdate in sorted(self.execTime[currentVersion]):
        avg = sum(self.execTime[currentVersion][realUpdate]) / len(self.execTime[currentVersion][realUpdate])
        resultTable += '%s %s %d\n' % (currentVersion, realUpdate, avg)

    stat = analyze.calcStat(self.execTime2)
    statResult = '%d %d %d %d %d\n' % stat

    return { 
      'profiling' : resultTable, 
      'exec_time_stat' : statResult,
    }
  
