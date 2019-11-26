from pyspark.ml.util import JavaMLReadable, JavaMLWritable, _jvm
from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol


class StringMap(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):
    def __init__(self, labels, inputCol=None, outputCol=None, handleInvalid='error', defaultValue=0.0):
        """
        __init__(self, labels, inputCol=None, outputCol=None, handleInvalid='error', defaultValue=0.0)
        labels is a dict {string: double}
        handleInvalid: how to handle missing labels: 'error' (throw an error), or 'keep' (map to the default value)
        """
        assert handleInvalid in ['error', 'keep'], 'Invalid value for handleInvalid: {}'.format(handleInvalid)
        super(StringMap, self).__init__()
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

    @classmethod
    def from_dataframe(cls, df, key_col, value_col, handleInvalid='error', defaultValue=0.0):
        """
        from_dataframe(self, df, key_col, value_col)
        Creates StringMap from a DataFrame.
        """
        dict_from_df = {row[0]: float(row[1]) for row in
                        df.select([key_col, value_col]).collect()}
        return cls(dict_from_df, inputCol=key_col, outputCol=value_col, handleInvalid=handleInvalid,
                   defaultValue=defaultValue)
