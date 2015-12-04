
class BaseAnalyzer(object):

  def __init__(self):
    pass

  def analyze(self, logLine, **kwargs):
    pass

  def analyzedResult(self):
    return {}

  def writeOutput(self, outputDir, suffix=''):
    suffix = '' if not suffix else '_' + suffix
    result = self.analyzedResult()
    for fileName in result:
      output = open('%s/%s%s' % (outputDir, fileName, suffix), 'w')
      output.write(result[fileName])
      output.close()

