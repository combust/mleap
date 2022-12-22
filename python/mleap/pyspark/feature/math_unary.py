from enum import Enum

from pyspark import keyword_only
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.ml.util import JavaMLReadable
from pyspark.ml.util import JavaMLWritable
from pyspark.ml.util import _jvm
from pyspark.ml.wrapper import JavaTransformer

from mleap.pyspark.py2scala import jvm_scala_object


class UnaryOperation(Enum):
    Sin = 1
    Cos = 2
    Tan = 3
    Log = 4
    Exp = 5
    Abs = 6
    Sqrt = 7
    Logit = 8


class MathUnary(JavaTransformer, HasInputCol, HasOutputCol, JavaMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, operation=None, inputCol=None, outputCol=None):
        """
        Computes the mathematical unary `operation` over the input column.

        NOTE: `operation` is not a JavaParam because the underlying 
        MathUnary scala object uses a MathUnaryModel to store the info about
        the unary operation (sin, tan, etc.), not a JavaParam string.

        `operation` has a None default value even though it should *never* be
        None. A None value is necessary upon deserialization to instantiate a
        MathUnary without errors. Afterwards, pyspark sets the _java_obj to
        the deserialized scala object, which encodes the operation.
        """
        super(MathUnary, self).__init__()

        # if operation=None, it means that pyspark is reloading the model
        # from disk and calling this method without args. In such case we don't
        # need to set _java_obj here because pyspark will set it after creation
        #
        # if operation is not None, we can proceed to instantiate the scala classes
        if operation:
            scalaUnaryOperation = jvm_scala_object(
                _jvm().ml.combust.mleap.core.feature,
                f"UnaryOperation${operation.name}$",
            )

            scalaMathUnaryModel = _jvm().ml.combust.mleap.core.feature.MathUnaryModel(scalaUnaryOperation)

            self._java_obj = self._new_java_obj(
                "org.apache.spark.ml.mleap.feature.MathUnary",
                self.uid,
                scalaMathUnaryModel,
            )

        self._setDefault()
        self.setParams(inputCol=inputCol, outputCol=outputCol)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        """
        Sets params for this MathUnary.
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
