
from .. import analyze
import pyutil

class TSilenceExtractor(analyze.BaseAnalyzer):

  def __init__(self):
    self.tsilence = []
    self.tsilenceAvg = []

  def analyze(self, logLine, **kwargs):
    if ' t_silence ' in logLine:
      tokens = logLine.split()
      self.tsilence.append(float(tokens[12]) / 1000)
      self.tsilenceAvg.append(float(tokens[14]) / 1000)

  def analyzedResult(self):
    return { 
      't_silence' : '\n'.join(map(str, self.tsilence)) + '\n',
      't_silence_avg' : '\n'.join(map(str, self.tsilenceAvg)) + '\n' 
    }
  
