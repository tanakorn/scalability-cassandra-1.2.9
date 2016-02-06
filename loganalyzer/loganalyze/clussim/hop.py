from .. import analyze

from ..realrun import hop
import pyutil

class HopCounter(hop.HopCounter):

  def analyze(self, logLine, **kwargs):
    if ' hop ' in logLine:
      tokens = logLine.split()
      hops = map(int, tokens[9][0:-1].split(','))
      self.numHop += len(hops)
      for hop in hops:
        if hop not in self.hopCount:
          self.hopCount[hop] = 0
        self.hopCount[hop] += 1


