
from .. import analyze

class LineCount(analyze.BaseAnalyzer):

  def __init__(self):
    self.outputName = 'linecount'
    self.count = {}

  def analyze(self, logLine, **kwargs):
    nid = kwargs['nid']
    if not nid in self.count:
      self.count[nid] = 0
    self.count[nid] += 1

  def analyzedResult(self):
    return str(self.count) + '\n'

