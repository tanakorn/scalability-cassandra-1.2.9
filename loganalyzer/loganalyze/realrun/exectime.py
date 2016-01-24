
from .. import analyze
import pyutil

class ExecTimeAnalyzer(analyze.BaseAnalyzer):

  def __init__(self):
    self.gossipType = ('gossip_ack', 'gossip_ack2', 'gossip_all')
    self.execTime = { 'gossip_ack' : [], 'gossip_ack2' : [], 'gossip_all' : [] }
    self.newVersion = { i : { 'boot' : [], 'normal' : [] } for i in self.gossipType }
    self.execTimeOfVersion = { i : { } for i in self.gossipType }
    self.execTimeOfVersionIndiv = { }
    self.realUpdate = []
    self.currentVersion = []

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
        realNormalUpdate = int(tokens[20])
        self.realUpdate.append(realNormalUpdate)
        currentVersion = int(tokens[22])
        self.currentVersion.append(currentVersion)

  def analyzedResult(self):
    result = { } 
    for execType in self.execTime:
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
      execTimes = self.execTimeOfVersionIndiv[normalVersion]
      mean, sd = analyze.calcAverage(execTimes)
      minV, lqV, medV, uqV, maxV = analyze.calcStat(execTimes)
      num = len(execTimes)
      result['exec_time_of_version_indiv'] += '%d %d %f %f %f %f %f %f %f\n' % (normalVersion, num, mean, sd, minV, lqV, medV, uqV, maxV);

    result['real_update'] = '\n'.join(map(str, self.realUpdate))

    result['current_version'] = '\n'.join(map(str, self.currentVersion))

    return result
  
