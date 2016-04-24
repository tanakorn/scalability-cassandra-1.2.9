from .. import analyze

from ..realrun import falsedetect
import pyutil

# The difference between real run and cluster simulation is how to parse log line
class FalseDetectionCounter(falsedetect.FalseDetectionCounter):

  def __init__(self):
    self.stableLine = None

  def analyze(self, logLine, **kwargs):
    if ' stable status yes ' in logLine:
      self.stableLine = logLine

  def analyzedResult(self):
    numFalse = -1
    if self.stableLine:
      numFalse = int(self.stableLine.split()[10])

    return {
      'num_false' : str(numFalse) + '\n',
    }
