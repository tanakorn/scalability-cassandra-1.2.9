from .. import analyze

from ..realrun import hop
import pyutil

class HopCounter(hop.HopCounter):

  def analyze(self, logLine, **kwargs):
    if ' hop ' in logLine:
      tokens = logLine.split()
      self.hops += map(int, tokens[9][0:-1].split(','))

