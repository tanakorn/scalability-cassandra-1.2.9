
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
    self.copyTime = []
    self.copyTimeOfNumCopy = {}
    self.copyTimeOfVersionAndTokensIndiv = { }
    self.copyTimeOfVersionAndRealUpdateAndTokensIndiv = { }
    self.updateTime = []
    self.updateTimeOfRealUpdateIndiv = { }
    self.updateTimeOfVersionAndRealUpdateAndTokensIndiv = { }
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

        copyTime = int(tokens[31])
        numCopy = normalVersion * currentTokens

        self.copyTime.append(str(copyTime))

        if numCopy not in self.copyTimeOfNumCopy:
          self.copyTimeOfNumCopy[numCopy] = []
        self.copyTimeOfNumCopy[numCopy].append(copyTime)

        if normalVersion not in self.copyTimeOfVersionAndTokensIndiv :
          self.copyTimeOfVersionAndTokensIndiv[normalVersion] = {}
        if currentTokens not in self.copyTimeOfVersionAndTokensIndiv[normalVersion]:
          self.copyTimeOfVersionAndTokensIndiv[normalVersion][currentTokens] = []
        self.copyTimeOfVersionAndTokensIndiv[normalVersion][currentTokens].append(copyTime)

        if normalVersion not in self.copyTimeOfVersionAndRealUpdateAndTokensIndiv:
          self.copyTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion] = {}
        if realNormalUpdate not in self.copyTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion]:
          self.copyTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realNormalUpdate] = {}
        if currentTokens not in self.copyTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realNormalUpdate]:
          self.copyTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realNormalUpdate][currentTokens] = []
        self.copyTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realNormalUpdate][currentTokens].append(copyTime)

        updateTime = int(tokens[33])

        self.updateTime.append(str(updateTime))

        if realNormalUpdate not in self.updateTimeOfRealUpdateIndiv:
          self.updateTimeOfRealUpdateIndiv[realNormalUpdate] = []
        self.updateTimeOfRealUpdateIndiv[realNormalUpdate].append(updateTime)

        if normalVersion not in self.updateTimeOfVersionAndRealUpdateAndTokensIndiv:
          self.updateTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion] = {}
        if realNormalUpdate not in self.updateTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion]:
          self.updateTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realNormalUpdate] = {}
        if currentTokens not in self.updateTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realNormalUpdate]:
          self.updateTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realNormalUpdate][currentTokens] = []
        self.updateTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realNormalUpdate][currentTokens].append(updateTime)

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

    result['copy_time'] = '\n'.join(self.copyTime) + '\n'

    result['copy_time_of_num_copy_indiv'] = ''
    for numCopy in sorted(self.copyTimeOfNumCopy.keys()):
      copyTimes = self.copyTimeOfNumCopy[numCopy]
      mean, sd = analyze.calcAverage(copyTimes)
      minV, lqV, medV, uqV, maxV = analyze.calcStat(copyTimes)
      num = len(copyTimes)
      result['copy_time_of_num_copy_indiv'] += '%d %d %f %f %f %f %f %f %f\n' % (numCopy, num, mean, sd, minV, lqV, medV, uqV, maxV);

    result['copy_time_of_version_and_tokens_indiv'] = ''
    for normalVersion in sorted(self.copyTimeOfVersionAndTokensIndiv.keys()):
      for currentTokens in sorted(self.copyTimeOfVersionAndTokensIndiv[normalVersion].keys()):
        copyTimes = self.copyTimeOfVersionAndTokensIndiv[normalVersion][currentTokens]
        mean, sd = analyze.calcAverage(copyTimes)
        minV, lqV, medV, uqV, maxV = analyze.calcStat(copyTimes)
        num = len(copyTimes)
        result['copy_time_of_version_and_tokens_indiv'] += '%d %d %d %f %f %f %f %f %f %f\n' % (normalVersion, currentTokens, num, mean, sd, minV, lqV, medV, uqV, maxV);

    result['copy_time_of_version_realupdate_tokens_indiv'] = ''
    for normalVersion in sorted(self.copyTimeOfVersionAndRealUpdateAndTokensIndiv.keys()):
      for realUpdate in sorted(self.copyTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion].keys()):
        for numTokens in sorted(self.copyTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realUpdate].keys()):
          copyTimes = self.copyTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realUpdate][numTokens]
          mean, sd = analyze.calcAverage(copyTimes)
          minV, lqV, medV, uqV, maxV = analyze.calcStat(copyTimes)
          num = len(copyTimes)
          result['copy_time_of_version_realupdate_tokens_indiv'] += '%d %d %d %d %f %f %f %f %f %f %f\n' % (normalVersion, realUpdate, numTokens, num, mean, sd, minV, lqV, medV, uqV, maxV);

    result['update_time'] = '\n'.join(self.updateTime) + '\n'

    result['update_time_of_realupdate_indiv'] = ''
    for realUpdate in sorted(self.updateTimeOfRealUpdateIndiv.keys()):
      updateTimes = self.updateTimeOfRealUpdateIndiv[realUpdate]
      mean, sd = analyze.calcAverage(updateTimes)
      minV, lqV, medV, uqV, maxV = analyze.calcStat(updateTimes)
      num = len(updateTimes)
      result['update_time_of_realupdate_indiv'] += '%d %d %f %f %f %f %f %f %f\n' % (realUpdate, num, mean, sd, minV, lqV, medV, uqV, maxV);

    result['update_time_of_version_realupdate_tokens_indiv'] = ''
    for normalVersion in sorted(self.updateTimeOfVersionAndRealUpdateAndTokensIndiv.keys()):
      for realUpdate in sorted(self.updateTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion].keys()):
        for numTokens in sorted(self.updateTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realUpdate].keys()):
          updateTimes = self.updateTimeOfVersionAndRealUpdateAndTokensIndiv[normalVersion][realUpdate][numTokens]
          mean, sd = analyze.calcAverage(updateTimes)
          minV, lqV, medV, uqV, maxV = analyze.calcStat(updateTimes)
          num = len(updateTimes)
          result['update_time_of_version_realupdate_tokens_indiv'] += '%d %d %d %d %f %f %f %f %f %f %f\n' % (normalVersion, realUpdate, numTokens, num, mean, sd, minV, lqV, medV, uqV, maxV);

    mean, sd = analyze.calcAverage(self.waitTime)
    num = len(self.waitTime)
    minVal = min(self.waitTime)
    maxVal = max(self.waitTime)
    result['wait_time'] = '%f, %f, %d, %d, %d\n' % (mean, sd, minVal, maxVal, num);

    return result
  
