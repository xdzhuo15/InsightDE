from pyspark import since, keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaTransformer, _jvm, JavaParams

from custom_spark_ml.utils.java_reader import CustomJavaMLReader


class FreqEncoder(JavaEstimator, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    Divides the range of a continuous column by an input parameter `numberBins` and then, for each row, decides the appropriate bin.
    """

    _classpath = 'com.custom.spark.feature.FreqEncoder'

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None ):
        super(FreqEncoder, self).__init__()
        self._java_obj = self._new_java_obj(
            FreqEncoder._classpath ,
            self.uid
        )
        self._setDefault(numberBins=10)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        """
        Set the params for the TokenDistributionVectorizer
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setOutputCol(self, value):
        return self._set(outputCol=value)

    def getOutputCol(self):
        return self.getOrDefault(self.outputCol)

    def _create_model(self, java_model):
        return FreqEncoderModel(java_model)


class FreqEncoderModel(JavaModel, JavaMLReadable, JavaMLWritable):
    """
    Model fitted by :py:class:`FreqEncoderizer`.
    """

    _classpath_model = 'com.custom.spark.feature.FreqEncoderModel'

    @property
    def bins(self):
        """
        Map containing the boundary points for the range of the bins
        """
        return self._call_java("javaBins")

    @staticmethod
    def _from_java(java_stage):
        """
        Given a Java object, create and return a Python wrapper of it.
        Used for ML persistence.

        Meta-algorithms such as Pipeline should override this method as a classmethod.
        """
        # Generate a default new instance from the stage_name class.
        py_type = FreqEncoderModel
        if issubclass(py_type, JavaParams):
            # Load information from java_stage to the instance.
            py_stage = py_type()
            py_stage._java_obj = java_stage
            py_stage._resetUid(java_stage.uid())
            py_stage._transfer_params_from_java()

        return py_stage

    @classmethod
    def read(cls):
        """Returns an MLReader instance for this class."""
        return CustomJavaMLReader(cls, cls._classpath_model)
