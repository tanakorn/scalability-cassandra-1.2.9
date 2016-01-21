
from .. import analyze
import pyutil

class MicroProfiling(analyze.BaseAnalyzer):

  def __init__(self):
    self.microProf = []
    self.updatedMicroProf = []
    self.nonUpdatedMicroProf = []

  def analyze(self, logLine, **kwargs):
    if ' Micro ' in logLine:
      tokens = logLine.split()
      data = map(lambda x: int(x.split('=')[1]), tokens[9:])
      self.microProf.append(data)
      isUpdated = tokens[16].split('=')[1] == '1'
      if isUpdated:
        self.updatedMicroProf.append(data)
      else:
        self.nonUpdatedMicroProf.append(data)

  def analyzedResult(self):
    result = {}
    pattern = '%d ' * len(self.microProf[0]) + '\n'

    result['all_micro_prof'] = ''
    for i in self.microProf:
      result['all_micro_prof'] += pattern % tuple(i)
    
    result['updated_micro_prof'] = ''
    for i in self.updatedMicroProf:
      result['updated_micro_prof'] += pattern % tuple(i)

    result['non_updated_micro_prof'] = ''
    for i in self.nonUpdatedMicroProf:
      result['non_updated_micro_prof'] += pattern % tuple(i)

    microProfTransp = zip(*self.microProf)
    avgMicroProf = []
    for entry in microProfTransp:
      mean, sd = analyze.calcAverage(entry)
      avgMicroProf.append(mean)
    result['avg_micro_prof'] = pattern % tuple(avgMicroProf)

    updatedMicroProfTransp = zip(*self.updatedMicroProf)
    avgUpdatedMicroProf = []
    for entry in updatedMicroProfTransp:
      mean, sd = analyze.calcAverage(entry)
      avgUpdatedMicroProf.append(mean)
    result['avg_updated_micro_prof'] = pattern % tuple(avgUpdatedMicroProf)
    
    nonUpdatedMicroProfTransp = zip(*self.nonUpdatedMicroProf)
    avgNonUpdatedMicroProf = []
    for entry in nonUpdatedMicroProfTransp:
      mean, sd = analyze.calcAverage(entry)
      avgNonUpdatedMicroProf.append(mean)
    if not nonUpdatedMicroProfTransp:
      avgNonUpdatedMicroProf = [ 0 ] * len(self.microProf[0])
    result['avg_non_updated_micro_prof'] = pattern % tuple(avgNonUpdatedMicroProf)
    
    return result
  
