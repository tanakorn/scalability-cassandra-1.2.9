
from ..realrun import phi
import pyutil

class PhiAnalyzer(phi.PhiAnalyzer):
  
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
    if ' allphi ' in logLine:
      tokens = logLine.split()
      if len(tokens) == 11:
        allPhi = tokens[10][:-1].split(',')
        self.allPhi += allPhi

