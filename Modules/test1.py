from pyspark import keyword_only
from pyspark.ml.param import Params

class SimpleAlgorithm(Params):

    @keyword_only
    def __init__(self, threshold=2.0):

        super(SimpleAlgorithm, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, threshold=2.0):

        kwargs = self._input_kwargs
        return self._set(**kwargs)


t = SimpleAlgorithm()

print t
