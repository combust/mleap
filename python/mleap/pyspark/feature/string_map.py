import six
from pyspark import keyword_only
from pyspark.ml.util import JavaMLReadable, JavaMLWritable, _jvm
from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql import DataFrame

from mleap.pyspark.py2scala import jvm_scala_object


class StringMap(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, labels={}, inputCol=None, outputCol=None, handleInvalid='error', defaultValue=0.0):
        """
        :param labels: a dict {string: double}
        :param handleInvalid: how to handle missing labels: 'error' (throw), or 'keep' (map to defaultValue)
        :param defaultValue: value to use if key is not found in labels
        """
        """
        labels must be a dict {string: double} or a spark DataFrame with columns inputCol & outputCol
        handleInvalid: 
        """
        super(StringMap, self).__init__()

        def validate_args():
            """
            validate args early to avoid failing at Py4j with some hard to interpret error message
            """
            assert handleInvalid in ['error', 'keep'], 'Invalid value for handleInvalid: {}'.format(handleInvalid)
            assert isinstance(labels, dict), 'labels must be a dict, got: {}'.format(type(labels))
            for (key, value) in labels.items():
                assert isinstance(key, six.string_types), \
                    'label keys must be a string type, got: {}'.format(type(key))
                assert isinstance(value, float), 'label values must be float, got: {}'.format(type(key))

        validate_args()

        labels_scala_map = _jvm() \
            .scala \
            .collection \
            .JavaConverters \
            .mapAsScalaMapConverter(labels) \
            .asScala() \
            .toMap(_jvm().scala.Predef.conforms())

        handle_invalid_jvm = jvm_scala_object(
            _jvm().ml.combust.mleap.core.feature,
            f"HandleInvalid${handleInvalid.capitalize()}$",
        )

        string_map_model = _jvm().ml.combust.mleap.core.feature.StringMapModel(
            labels_scala_map,
            handle_invalid_jvm,
            defaultValue,
        )

        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.mleap.feature.StringMap",
            self.uid,
            string_map_model
        )

        self._setDefault()
        self.setParams(inputCol=inputCol, outputCol=outputCol)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        """
        setParams(self, inputCol=None, outputCol=None)
        Sets params for this StringMap.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCol(self, value):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    def setOutputCol(self, value):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    @classmethod
    def from_dataframe(cls, labels_df, inputCol, outputCol, handleInvalid='error', defaultValue=0.0):
        """
        :param labels_df: a spark DataFrame whose columns include inputCol:string & outputCol:double.
        See StringMap() for other params.
        """
        assert isinstance(labels_df, DataFrame), 'labels must be a DataFrame, got: {}'.format(type(labels_df))
        labels_dict = {
            row[0]: float(row[1])
            for row in
            labels_df.select([inputCol, outputCol]).collect()
        }

        return cls(
            labels=labels_dict,
            inputCol=inputCol,
            outputCol=outputCol,
            handleInvalid=handleInvalid,
            defaultValue=defaultValue
        )

