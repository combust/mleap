from enum import Enum

from pyspark import keyword_only
from pyspark.ml.param.shared import HasInputCol
from pyspark.ml.param.shared import HasOutputCol
from pyspark.ml.param.shared import Param
from pyspark.ml.param.shared import Params
from pyspark.ml.param.shared import TypeConverters
from pyspark.ml.util import JavaMLReadable
from pyspark.ml.util import JavaMLWritable
from pyspark.ml.util import _jvm
from pyspark.ml.wrapper import JavaTransformer

from mleap.pyspark.py2scala import jvm_scala_object


class BinaryOperation(Enum):
    Add = 1
    Subtract = 2
    Multiply = 3
    Divide = 4
    Remainder = 5
    LogN = 6
    Pow = 7


class MathBinary(JavaTransformer, HasOutputCol, JavaMLReadable, JavaMLWritable):

    inputA = Param(
        Params._dummy(),
        "inputA",
        "the inputA column name",
        typeConverter=TypeConverters.toString,
    )

    inputB = Param(
        Params._dummy(),
        "inputB",
        "the inputB column name",
        typeConverter=TypeConverters.toString,
    )

    @keyword_only
    def __init__(self, operation=None, inputA=None, inputB=None, outputCol=None):
        """
        Computes the mathematical binary `operation` over the input column.

        NOTE: we can't make `operation` a JavaParam (as in pyspark) because the
            underlying scala object MathBinary uses a MathBinaryModel to store
            the info about the unary operation (add, mul, etc.)

            If operation is a JavaParam, py4j will fail trying to set it on the
            underlying scala object.

            If operation doesn't have a default value, then pyspark will fail
            upon deserialization trying to instantiate this object without args:
                (it just runs py_type() where py_type is the class name)

        """
        super(MathBinary, self).__init__()

        # if operation=None, it means that pyspark is reloading the model
        # from disk and calling this method without args. In such case we don't
        # need to set _java_obj here because pyspark will set it after creation
        #
        # if operation is not None, we can proceed to instantiate the scala classes
        if operation:
            scalaBinaryOperation = jvm_scala_object(
                _jvm().ml.combust.mleap.core.feature.BinaryOperation,
                operation.name
            )

            scalaMathBinaryModel = _jvm().ml.combust.mleap.core.feature.MathBinaryModel(
                scalaBinaryOperation, None, None
            )

            self._java_obj = self._new_java_obj(
                "org.apache.spark.ml.mleap.feature.MathBinary",
                self.uid,
                scalaMathBinaryModel,
            )

        self._setDefault()
        self.setParams(inputA=inputA, inputB=inputB, outputCol=outputCol)

    @keyword_only
    def setParams(self, inputA=None, inputB=None, outputCol=None):
        """
        Sets params for this MathBinary.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    # def setInputCol(self, value):
        # """
        # Sets the value of :py:attr:`inputCol`.
        # """
        # return self._set(inputCol=value)

    # def setOutputCol(self, value):
        # """
        # Sets the value of :py:attr:`outputCol`.
        # """
        # return self._set(outputCol=value)

