
class BaseAnalyzer(object):

  def __init__(self):
    self.outputName = 'base'

  def analyze(self, logLine, **kwargs):
    pass

  def analyzedResult(self):
    return ''

  def writeOutput(self, outputDir, suffix=''):
    suffix = '' if not suffix else '_' + suffix
    output = open('%s/%s%s' % (outputDir, self.outputName, suffix), 'w')
    output.write(self.analyzedResult())
    output.close()

