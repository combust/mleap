from pyspark.ml.util import JavaMLReadable, JavaMLWritable, _jvm
from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql import DataFrame


class StringMap(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    def __init__(self, labels, inputCol=None, outputCol=None, handleInvalid='error', defaultValue=0.0):
        """
        __init__(self, labels, inputCol=None, outputCol=None, handleInvalid='error', defaultValue=0.0)
        labels must be a dict {string: double} or a spark DataFrame with columns inputCol & outputCol
        handleInvalid: how to handle missing labels: 'error' (throw an error), or 'keep' (map to the default value)
        """
        super(StringMap, self).__init__()

        if isinstance(labels, DataFrame):
            assert inputCol, 'inputCol is required when labels is a DataFrame'
            assert outputCol, 'outputCol is required when labels is a DataFrame'
            labels = {row[0]: float(row[1]) for row in labels.select([inputCol, outputCol]).collect()}

        def validate_args():
            """
            validate args early to avoid failing at Py4j with some hard to interpret error message
            """
            assert handleInvalid in ['error', 'keep'], 'Invalid value for handleInvalid: {}'.format(handleInvalid)
            assert isinstance(labels, dict), 'labels must be a dict or a DataFrame'
            for (key, value) in labels.items():
                assert isinstance(key, str), 'label keys must be string'
                assert isinstance(value, float), 'label values must be float'

        validate_args()

        labels_scala_map = _jvm() \
            .scala \
            .collection \
            .JavaConverters \
            .mapAsScalaMapConverter(labels) \
            .asScala() \
            .toMap(_jvm().scala.Predef.conforms())
        handle_invalid_jvm = _jvm().ml.combust.mleap.core.feature.StringMapHandleInvalid.__getattr__(
            handleInvalid.capitalize() + '$').__getattr__('MODULE$')
        string_map_model = self._new_java_obj("ml.combust.mleap.core.feature.StringMapModel",
                                              labels_scala_map, handle_invalid_jvm, defaultValue)
        self._java_obj = self._new_java_obj("org.apache.spark.ml.mleap.feature.StringMap", self.uid, string_map_model)
        self.setInputCol(inputCol)
        self.setOutputCol(outputCol)
