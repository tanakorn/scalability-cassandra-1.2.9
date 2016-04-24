
from ..realrun import phi
import pyutil

class PhiAnalyzer(phi.PhiAnalyzer):

  def __init__(self):
    self.maxphiLine = None
  
  def analyze(self, logLine, **kwargs):
    if 'all maxphi' in logLine:
      self.maxphiLine = logLine

  def analyzedResult(self):
    maxphi = self.maxphiLine.split()[9][:-1].split(',')
    return {
      'maxphi_of_observees' : '\n'.join(maxphi),
    }
