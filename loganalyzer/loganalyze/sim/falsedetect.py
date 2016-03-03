from .. import analyze

from ..real import falsedetect
import pyutil

# The difference between real run and cluster simulation is how to parse log line
class FalseDetectionCounter(falsedetect.FalseDetectionCounter):

  def analyze(self, logLine, **kwargs):
    tokens = logLine.split()
    timestamp = analyze.extractTimestamp(tokens)
    if not self.startTimestamp or timestamp < self.startTimestamp:
      self.startTimestamp = timestamp
    if not self.endTimestamp or timestamp > self.endTimestamp:
      self.endTimestamp = timestamp
    if ' convict ' in logLine:
      observer = pyutil.ip2nid[tokens[7][1:]]
      observee = pyutil.ip2nid[tokens[9][1:]]
      self.falseMap[observer][observee].append(timestamp)
      self.revertFalseMap[observee][observer].append(timestamp)

