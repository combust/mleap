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
from mleap.pyspark.py2scala import ScalaNone
from mleap.pyspark.py2scala import Some


class BinaryOperation(Enum):
    Add = 1
    Subtract = 2
    Multiply = 3
    Divide = 4
    Remainder = 5
    LogN = 6
    Pow = 7
    Min = 8
    Max = 9


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
            defaultA=None,
            defaultB=None,
    ):
        """
        Computes the mathematical binary `operation` over
        the input columns A and B.

        :param operation: BinaryOperation to specify the operation type
        :param inputA: column name for the left side of operation (string)
        :param inputB: column name for the right side of operation (string)
        :param outputCol: output column name (string)
        :param defaultA: Default to use instead of inputA. This will only be used
         when inputA is None. For example when defaultA=4,
         operation=BinaryOperation.Multiply and inputB=f1, then all entries of
         col f1 will be multiplied by 4.
        :param defaultB: Default to use instead of inputB. This will only be used
         when inputB is None. For example when defaultB=4,
         operation=BinaryOperation.Multiply and inputA=f1, then all entries of
         col f1 will be multiplied by 4.

        NOTE: `operation`, `defaultA`, `defaultB` is not a JavaParam because
        the underlying MathBinary scala object uses a MathBinaryModel to store
        the info about the binary operation.

        `operation` has a None default value even though it should *never* be
        None. A None value is necessary upon deserialization to instantiate a
        MathBinary without errors. Afterwards, pyspark sets the _java_obj to
        the deserialized scala object, which encodes the operation (as well
        as the default values for A and B).
        """
        super(MathBinary, self).__init__()

        # if operation=None, it means that pyspark is reloading the model
        # from disk and calling this method without args. In such case we don't
        # need to set _java_obj here because pyspark will set it after creation
        #
        # if operation is not None, we can proceed to instantiate the scala classes
        if operation:
            scalaBinaryOperation = jvm_scala_object(
                _jvm().ml.combust.mleap.core.feature,
                f"BinaryOperation${operation.name}$"
            )

            scalaMathBinaryModel = _jvm().ml.combust.mleap.core.feature.MathBinaryModel(
                scalaBinaryOperation,
                Some(defaultA) if defaultA is not None else ScalaNone(),
                Some(defaultB) if defaultB is not None else ScalaNone(),
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
        # For the correct behavior of MathBinary, params that are None must be unset
        kwargs = {k: v for k, v in self._input_kwargs.items() if v is not None}
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
