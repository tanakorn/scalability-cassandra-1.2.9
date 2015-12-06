
from .. import analyze
import pyutil

class PhiAnalyzer(analyze.BaseAnalyzer):
  
  def __init__(self):
    # this is a nested map that maps observer -> observee -> phi list
    self.phiMap = { i : { j : [] for j in pyutil.nid2ip.keys() } for i in pyutil.nid2ip.keys() }
    self.revertPhiMap = { i : { j : [] for j in pyutil.nid2ip.keys() } for i in pyutil.nid2ip.keys() }

  def analyze(self, logLine, **kwargs):
    if ' PHI ' in logLine:
      tokens = logLine.split()
      observeeIp = tokens[9][1:]
      observee = pyutil.ip2nid[observeeIp]
      observerIp = tokens[11][1:]
      observer = pyutil.ip2nid[observerIp]
      phi = float(tokens[13])
      self.phiMap[observer][observee].append(phi)
      self.revertPhiMap[observee][observer].append(phi)

  def analyzedResult(self):
    # I should do something smart here
    allMaxPhi = []
    maxPhiMap = {}
    maxPhiInObserversResult = ''
    for observer in self.phiMap:
      maxPhiMap[observer] = {}
      maxPhiInObserver = (0, 0)
      for observee in self.phiMap[observer]:
        if observer == observee:
          continue
        thisMaxPhi = max(self.phiMap[observer][observee])
        maxPhiMap[observer][observee] = thisMaxPhi
        allMaxPhi.append(str(thisMaxPhi))
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
        thisMaxPhi = max(self.revertPhiMap[observee][observer])
        revertMaxPhiMap[observee][observer] = thisMaxPhi
        if maxPhiOfObservee[1] < thisMaxPhi:
          maxPhiOfObservee = (observer, thisMaxPhi)
      maxPhiOfObserveesResult += '%d %d %f\n' % (observee, maxPhiOfObservee[0], maxPhiOfObservee[1])

    return { 
        'maxphi' : '\n'.join(allMaxPhi) + '\n',
        'maxphi_in_observers' : maxPhiInObserversResult,
        'maxphi_of_observees' : maxPhiOfObserveesResult,
    }

