import importlib

# filename : classname
modClassMap = { 
  'phi' : 'PhiAnalyzer',
  'falsedetect' : 'FalseDetectionCounter',
  'exectime' : 'ExecTimeAnalyzer',
  'hop' : 'HopCounter',
  'tsilence' : 'TSilenceExtractor',
  'message' : 'MessageCounter',
  'lateness' : 'LatenessCounter',
  'stable' : 'StableAnalyzer',
}

modClassMap = { '%s.%s' % (__name__, i) : modClassMap[i] for i in modClassMap }
allMods = map(importlib.import_module, modClassMap)
allAnalyzers = [ getattr(i, modClassMap[i.__name__])() for i in allMods ]

def analyze(logLine, **kwargs):
  map(lambda x: x.analyze(logLine, **kwargs), allAnalyzers)

def writeOutput(logLine, prefix=''):
  map(lambda x: x.writeOutput(logLine, prefix), allAnalyzers)

