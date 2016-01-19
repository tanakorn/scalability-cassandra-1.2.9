
from .. import analyze
import pyutil

class ExecTimeAnalyzer(analyze.BaseAnalyzer):

  def __init__(self):
    self.gossipType = ('gossip_ack', 'gossip_ack2', 'gossip_all')
    self.execTime = { 'gossip_ack' : [], 'gossip_ack2' : [], 'gossip_all' : [] }
    self.newVersion = { i : { 'boot' : [], 'normal' : [] } for i in self.gossipType }
    self.execTimeOfVersion = { i : { } for i in self.gossipType }
    self.execTimeOfVersionIndiv = { }
    self.execTimeOfRealUpdateIndiv = { }
    self.execTimeOfRealUpdateAndTokensIndiv = { }
    self.execTimeOfVersionAndRealUpdateAndTokensIndiv = { }
    self.waitTime = []

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
        if realNormalUpdate not in self.execTimeOfRealUpdateIndiv:
          self.execTimeOfRealUpdateIndiv[realNormalUpdate] = []
        self.execTimeOfRealUpdateIndiv[realNormalUpdate].append(execTime)
        currentTokens = int(tokens[26])
        if realNormalUpdate not in self.execTimeOfRealUpdateAndTokensIndiv:
          self.execTimeOfRealUpdateAndTokensIndiv[realNormalUpdate] = {}
        if currentTokens not in self.execTimeOfRealUpdateAndTokensIndiv[realNormalUpdate]:
          self.execTimeOfRealUpdateAndTokensIndiv[realNormalUpdate][currentTokens] = []
        self.execTimeOfRealUpdateAndTokensIndiv[realNormalUpdate][currentTokens].append(execTime)
        if normalVersion not in self.execTimeOfVersionAndRealUpdateAndTokensIndiv:
          self.execTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion] = {}
        if realNormalUpdate not in self.execTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion]:
          self.execTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realNormalUpdate] = {}
        if currentTokens not in self.execTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realNormalUpdate]:
          self.execTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realNormalUpdate][currentTokens] = []
        self.execTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realNormalUpdate][currentTokens].append(execTime)
        self.waitTime.append(int(tokens[23]))

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

    result['exec_time_of_realupdate_indiv'] = ''
    for normalVersion in sorted(self.execTimeOfRealUpdateIndiv.keys()):
      execTimes = self.execTimeOfRealUpdateIndiv[normalVersion]
      mean, sd = analyze.calcAverage(execTimes)
      minV, lqV, medV, uqV, maxV = analyze.calcStat(execTimes)
      num = len(execTimes)
      result['exec_time_of_realupdate_indiv'] += '%d %d %f %f %f %f %f %f %f\n' % (normalVersion, num, mean, sd, minV, lqV, medV, uqV, maxV);

    result['exec_time_of_realupdate_tokens_indiv'] = ''
    for realUpdate in sorted(self.execTimeOfRealUpdateAndTokensIndiv.keys()):
      for numTokens in sorted(self.execTimeOfRealUpdateAndTokensIndiv[realUpdate].keys()):
        execTimes = self.execTimeOfRealUpdateAndTokensIndiv[realUpdate][numTokens]
        mean, sd = analyze.calcAverage(execTimes)
        minV, lqV, medV, uqV, maxV = analyze.calcStat(execTimes)
        num = len(execTimes)
        result['exec_time_of_realupdate_tokens_indiv'] += '%d %d %d %f %f %f %f %f %f %f\n' % (realUpdate, numTokens, num, mean, sd, minV, lqV, medV, uqV, maxV);

    result['exec_time_of_version_realupdate_tokens_indiv'] = ''
    for normalVersion in sorted(self.execTimeOfVersionAndRealUpdateAndTokensIndiv.keys()):
      for realUpdate in sorted(self.execTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion].keys()):
        for numTokens in sorted(self.execTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realUpdate].keys()):
          execTimes = self.execTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realUpdate][numTokens]
          mean, sd = analyze.calcAverage(execTimes)
          minV, lqV, medV, uqV, maxV = analyze.calcStat(execTimes)
          num = len(execTimes)
          result['exec_time_of_version_realupdate_tokens_indiv'] += '%d %d %d %d %f %f %f %f %f %f %f\n' % (normalVersion, realUpdate, numTokens, num, mean, sd, minV, lqV, medV, uqV, maxV);

    mean, sd = analyze.calcAverage(self.waitTime)
    num = len(self.waitTime)
    minVal = min(self.waitTime)
    maxVal = max(self.waitTime)
    result['wait_time'] = '%f, %f, %d, %d, %d\n' % (mean, sd, minVal, maxVal, num);

    return result
  
