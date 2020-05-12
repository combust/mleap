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
from mleap.pyspark.py2scala import Some


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
        "input for left side of binary operation",
        typeConverter=TypeConverters.toString,
    )

    inputB = Param(
        Params._dummy(),
        "inputB",
        "input for right side of binary operation",
        typeConverter=TypeConverters.toString,
    )

    @keyword_only
    def __init__(
        self,
        operation=None,
        inputA=None,
        inputB=None,
        outputCol=None,
    ):
        """
        Computes the mathematical binary `operation` over
        the input columns A and B.

        :param operation: BinaryOperation to specify the operation type
        :param inputA: column name for the left side of operation (string)
        :param inputB: column name for the right side of operation (string)
        :param outputCol: output column name (string)

        NOTE: `operation` is not a JavaParam because the underlying MathBinary
        scala object uses a MathBinaryModel to store the info about the binary
        operation.

        `operation` has a None default value even though it should *never* be
        None. A None value is necessary upon deserialization to instantiate a
        MathBinary without errors. Afterwards, pyspark sets the _java_obj to
        the deserialized scala object, which encodes the operation.
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

            # IMPORTANT: defaults for missing values are forced to None.
            # I've found an issue when setting default values for A and B,
            # Remember to treat your missing values before the MathBinary
            # (for example, you could use an Imputer)
            scalaMathBinaryModel = _jvm().ml.combust.mleap.core.feature.MathBinaryModel(
                scalaBinaryOperation, Some(None), Some(None)
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

    def setInputA(self, value):
        """
        Sets the value of :py:attr:`inputA`.
        """
        return self._set(inputA=value)

    def setInputB(self, value):
        """
        Sets the value of :py:attr:`inputB`.
        """
        return self._set(inputB=value)

    def setOutputCol(self, value):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

