
from .. import analyze
import pyutil

class PhiAnalyzer(analyze.BaseAnalyzer):
  
  def __init__(self):
    # this is a nested map that maps observer -> observee -> phi list
    self.phiMap = { i : { j : [] for j in pyutil.nid2ip.keys() } for i in pyutil.nid2ip.keys() }
    self.revertPhiMap = { i : { j : [] for j in pyutil.nid2ip.keys() } for i in pyutil.nid2ip.keys() }
    self.phiCount = {}
    self.numPhi = 0.0

  def analyze(self, logLine, **kwargs):
    observer = kwargs['nid']
    if ' PHI ' in logLine:
      tokens = logLine.split()
      observeeIp = tokens[9][1:]
      observee = pyutil.ip2nid[observeeIp]
      phi = float(tokens[11])
      self.phiMap[observer][observee].append(phi)
      self.revertPhiMap[observee][observer].append(phi)
    if ' allphi ' in logLine:
      tokens = logLine.split()
      if len(tokens) == 11:
        allPhi = map(float, tokens[10][:-1].split(','))
        self.numPhi += len(allPhi)
        for phi in allPhi:
          roundPhi = round(phi, 2)
          if roundPhi not in self.phiCount:
            self.phiCount[roundPhi] = 0
          self.phiCount[roundPhi] += 1

  def analyzedResult(self):
    # I should do something smart here
    allMaxPhi = []
    maxPhiMap = {}
    #maxPhiInObservers = {}
    maxPhiInObserversResult = ''
    for observer in self.phiMap:
      maxPhiMap[observer] = {}
      #maxPhiInObservers[observer] = (0, 0)
      maxPhiInObserver = (0, 0)
      for observee in self.phiMap[observer]:
        if observer == observee:
          continue
        thisMaxPhi = 0 if not self.phiMap[observer][observee] else max(self.phiMap[observer][observee])
        maxPhiMap[observer][observee] = thisMaxPhi
        allMaxPhi.append(str(thisMaxPhi))
        #if maxPhiInObservers[observer][1] < thisMaxPhi:
          #maxPhiInObservers[observer] = (observee, thisMaxPhi)
        if maxPhiInObserver[1] < thisMaxPhi:
          maxPhiInObserver = (observee, thisMaxPhi)
      maxPhiInObserversResult += '%d %d %f\n' % (observer, maxPhiInObserver[0], maxPhiInObserver[1])

    revertMaxPhiMap = {}
    maxPhiOfObserveesResult = ''
    for observee in self.revertPhiMap:
      revertMaxPhiMap[observee] = {}
      maxPhiOfObservee = (0, 0)
      for observer in self.revertPhiMap[observee]:
        if observer == observee:
          continue
        thisMaxPhi = 0 if not self.revertPhiMap[observee][observer] else max(self.revertPhiMap[observee][observer])
        revertMaxPhiMap[observee][observer] = thisMaxPhi
        if maxPhiOfObservee[1] < thisMaxPhi:
          maxPhiOfObservee = (observer, thisMaxPhi)
      maxPhiOfObserveesResult += '%d %d %f\n' % (observee, maxPhiOfObservee[0], maxPhiOfObservee[1])

    allPhiCdfReport = ''
    cumulative = 0.0
    for phi in sorted(self.phiCount):
      thisPhiPercent = self.phiCount[phi] / self.numPhi
      cumulative += thisPhiPercent
      allPhiCdfReport += '%f %f %f\n' % (phi, thisPhiPercent, cumulative)

    return { 
        'maxphi' : '\n'.join(allMaxPhi) + '\n',
        'maxphi_in_observers' : maxPhiInObserversResult,
        'maxphi_of_observees' : maxPhiOfObserveesResult,
        'allphi_cdf' : allPhiCdfReport,
    }

