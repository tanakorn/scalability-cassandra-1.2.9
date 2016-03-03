
from .. import analyze

class LineCount(analyze.BaseAnalyzer):

  def __init__(self):
    self.count = 0

  def analyze(self, logLine, **kwargs):
    self.count += 1

  def analyzedResult(self):
    return { 'linecount' : str(self.count) + '\n' }

