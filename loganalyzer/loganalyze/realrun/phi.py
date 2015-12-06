
from .. import analyze
import pyutil

class PhiAnalyzer(analyze.BaseAnalyzer):
  
  def __init__(self):
    # this is a nested map that maps observer -> observee -> phi list
    self.phiMap = {}

  def analyze(self, logLine, **kwargs):
    observer = kwargs['nid']
    if ' PHI ' in logLine:
      tokens = logLine.split()
      observeeIp = tokens[9][1:]
      observee = pyutil.ip2nid[observeeIp]
      phi = float(tokens[11])
      if observer not in self.phiMap:
        self.phiMap[observer] = {}
      if observee not in self.phiMap[observer]:
        self.phiMap[observer][observee] = []
      self.phiMap[observer][observee].append(phi)

  def analyzedResult(self):
    # I should do something smart here
    allMaxPhi = []
    for observer in self.phiMap:
      for observee in self.phiMap[observer]:
        allMaxPhi.append(str(max(self.phiMap[observer][observee])))
    return { 'maxphi' : '\n'.join(allMaxPhi) + '\n' }

