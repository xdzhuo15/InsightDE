import json
from pyspark import keyword_only
from pyspark.ml.pipeline import Estimator, Model, Pipeline, Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.sql.functions import udf, count
from io_modules import CountOutput
from pyspark.sql.types import MapType, StringType, IntegerType

# Create a custom word count transformer class
class FreqEncoder(Estimator, HasInputCol, HasOutputCol):

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(FreqEncoder, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
        self.bins = dict()

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def exportBins(self, bins):
        object = CountOutput()
        with open(object.output_name(), 'w') as f:
            json.dump(bins, f)

    def _fit(self, dataset):

        c = self.getInputCol()

        bins = dataset.groupBy(c).agg(count(c)).collect()

        bin_dict = {}
        for row in bins:
            bin_dict[row[0]] = row[1]

        in_col = dataset[self.getInputCol()]

        self.exportBins(bin_dict)

        return (FreqEncoderModel()
               .setInputCol(c)
               .setOutputCol(self.getOutputCol()))


class FreqEncoderModel(Transformer, HasInputCol, HasOutputCol):
    def readBin(self):
        object = CountOutput()
        bins = object.read_file()
        return bins

    def _transform(self, dataset):

        def mapValue(value, bins):
            if value is None or value not in bins:
                return 0
            else:
                return bins[value]

        out_col = self.getOutputCol()
        in_col = dataset[self.getInputCol()]
        # Define transformer
        bins = self.readBin()
        udf_map = udf(lambda value: mapValue(value, bins), IntegerType())

        return dataset.withColumn(out_col, udf_map(in_col))
