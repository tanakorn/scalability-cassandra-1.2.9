
from .. import analyze
import pyutil

class LatenessCounter(analyze.BaseAnalyzer):

  def __init__(self):
    self.lateCount = 0

  def analyze(self, logLine, **kwargs):
    if ' more than 1 s ' in logLine:
      self.lateCount += 1

  def analyzedResult(self):
    return { 'lateness' : str(self.lateCount) }
  
