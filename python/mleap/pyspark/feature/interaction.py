from enum import Enum

from pyspark import keyword_only
from pyspark.ml.param.shared import HasInputCols
from pyspark.ml.param.shared import HasOutputCol
from pyspark.ml.util import JavaMLReadable
from pyspark.ml.util import JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer


class Interaction(JavaTransformer, HasInputCols, HasOutputCol, JavaMLReadable, JavaMLWritable):
    """
    NOTE: copied from: Spark 3.0:
        https://github.com/apache/spark/blob/branch-3.0/python/pyspark/ml/feature.py#L1702

    Implements the feature interaction transform. This transformer takes in Double and Vector type
    columns and outputs a flattened vector of their feature interactions. To handle interaction,
    we first one-hot encode any nominal features. Then, a vector of the feature cross-products is
    produced.
    For example, given the input feature values `Double(2)` and `Vector(3, 4)`, the output would be
    `Vector(6, 8)` if all input features were numeric. If the first feature was instead nominal
    with four categories, the output would then be `Vector(0, 0, 0, 0, 3, 4, 0, 0)`.
    >>> df = spark.createDataFrame([(0.0, 1.0), (2.0, 3.0)], ["a", "b"])
    >>> interaction = Interaction()
    >>> interaction.setInputCols(["a", "b"])
    Interaction...
    >>> interaction.setOutputCol("ab")
    Interaction...
    >>> interaction.transform(df).show()
    +---+---+-----+
    |  a|  b|   ab|
    +---+---+-----+
    |0.0|1.0|[0.0]|
    |2.0|3.0|[6.0]|
    +---+---+-----+
    ...
    >>> interactionPath = temp_path + "/interaction"
    >>> interaction.save(interactionPath)
    >>> loadedInteraction = Interaction.load(interactionPath)
    >>> loadedInteraction.transform(df).head().ab == interaction.transform(df).head().ab
    True
    """

    @keyword_only
    def __init__(self, inputCols=None, outputCol=None):
        """
        __init__(self, inputCols=None, outputCol=None):
        """

        super(Interaction, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.feature.Interaction", self.uid
        )
        self._setDefault()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCols=None, outputCol=None):
        """
        setParams(self, inputCols=None, outputCol=None)
        Sets params for this Interaction.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCols(self, value):
        """
        Sets the value of :py:attr:`inputCols`.
        """
        return self._set(inputCols=value)

    def setOutputCol(self, value):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)
