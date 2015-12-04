
from .. import analyze

class LineCount(analyze.BaseAnalyzer):

  def __init__(self):
    self.outputName = 'linecount'
    self.count = 0

  def analyze(self, logLine):
    self.count += 1

  def analyzedResult(self):
    return str(self.count) + '\n'

