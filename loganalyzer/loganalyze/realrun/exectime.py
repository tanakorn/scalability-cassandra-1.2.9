
from .. import analyze
import pyutil

class ExecTimeAnalyzer(analyze.BaseAnalyzer):

  def __init__(self):
    self.gossipType = ('gossip_ack', 'gossip_ack2', 'gossip_all')
    self.execTime = { 'gossip_ack' : [], 'gossip_ack2' : [], 'gossip_all' : [] }
    self.newVersion = { i : { 'boot' : [], 'normal' : [] } for i in self.gossipType }
    self.execTimeOfVersion = { i : { } for i in self.gossipType }
    self.execTimeOfVersionIndiv = { }

  def analyze(self, logLine, **kwargs):
    if ' executes ' in logLine:
      tokens = logLine.split()
      #observerIp = tokens[7][1:]
      #observer = pyutil.ip2nid[observerIp]
      execTime = float(tokens[11]) / 1000
      execType = tokens[9]
      self.execTime[execType].append(execTime)
      self.newVersion[execType]['boot'].append(tokens[16])
      self.newVersion[execType]['normal'].append(tokens[18])
      execTimeOfVersion = self.execTimeOfVersion[execType]
      normalVersion = int(tokens[18])
      if normalVersion not in execTimeOfVersion:
        execTimeOfVersion[normalVersion] = []
      execTimeOfVersion[normalVersion].append(execTime)
      if execType != 'gossip_all':
        if normalVersion not in self.execTimeOfVersionIndiv:
          self.execTimeOfVersionIndiv[normalVersion] = []
        self.execTimeOfVersionIndiv[normalVersion].append(execTime)

  def analyzedResult(self):
    result = { } 
    for execType in self.execTime:
      #minVal, lqVal, medVal, uqVal, maxVal = analyze.calcStat(self.execTime[execType])
      #mean = sum(self.execTime[execType]) / len(self.execTime[execType])
      #result += '%s mean=%f min=%f lq=%f med=%f uq=%f max=%f\n' % (execType, mean, minVal, lqVal, medVal, uqVal, maxVal)
      result['exec_time_' + execType] = '\n'.join(map(str, self.execTime[execType])) + '\n'
    for execType in self.gossipType:
      result['new_boot_' + execType] = '\n'.join(self.newVersion[execType]['boot']) + '\n'
      result['new_normal_' + execType] = '\n'.join(self.newVersion[execType]['normal']) + '\n'
      
      execTimeOfVersion = self.execTimeOfVersion[execType]
      result['exec_time_of_version_' + execType] = ''
      for normalVersion in sorted(execTimeOfVersion.keys()):
        mean = sum(execTimeOfVersion[normalVersion]) / len(execTimeOfVersion[normalVersion])
        minVal = min(execTimeOfVersion[normalVersion])
        maxVal = max(execTimeOfVersion[normalVersion])
        result['exec_time_of_version_' + execType] += '%d %f %f %f\n' % (normalVersion, mean, minVal, maxVal)

    result['exec_time_of_version_indiv'] = ''
    for normalVersion in sorted(self.execTimeOfVersionIndiv.keys()):
      mean = sum(self.execTimeOfVersionIndiv[normalVersion]) / len(self.execTimeOfVersionIndiv[normalVersion])
      minVal = min(self.execTimeOfVersionIndiv[normalVersion])
      maxVal = max(self.execTimeOfVersionIndiv[normalVersion])
      result['exec_time_of_version_indiv'] += '%d %f %f %f\n' % (normalVersion, mean, minVal, maxVal)

    return result
  
