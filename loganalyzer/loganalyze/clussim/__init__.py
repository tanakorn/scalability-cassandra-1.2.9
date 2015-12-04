import importlib

# filename : classname
modClassMap = { 
  'linecount' : 'LineCount',
}

modClassMap = { '%s.%s' % (__name__, i) : modClassMap[i] for i in modClassMap }
allMods = map(importlib.import_module, modClassMap)
allAnalyzers = [ getattr(i, modClassMap[i.__name__])() for i in allMods ]

def analyze(logLine):
  map(lambda x: x.analyze(logLine), allAnalyzers)

def writeOutput(logLine, prefix=''):
  map(lambda x: x.writeOutput(logLine, prefix), allAnalyzers)

