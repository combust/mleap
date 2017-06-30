package ml.combust.mleap.runtime.function

import ml.combust.mleap.core.types._
import ml.combust.mleap.tensor.Tensor
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/21/16.
  */
class UserDefinedFunctionSpec extends FunSpec {
  describe("#apply") {
    it("creates the udf") {
      val udf0: UserDefinedFunction = () => "hello"
      val udf1: UserDefinedFunction = (_: Double) => Seq("hello")
      val udf2: UserDefinedFunction = (v1: Long, v2: Int) => v1 + v2
      val udf3: UserDefinedFunction = (_: Boolean, _: Tensor[Double]) => "hello": Any
      val udf4: UserDefinedFunction = (_: Seq[Boolean], _: Option[Seq[String]], _: Seq[Double]) => "hello"
      val udf5: UserDefinedFunction = (_: Double, _: Float, _: Double, _: Double, _: String) => 55d

      assertUdfForm(udf0, StringType())
      assertUdfForm(udf1, ListType(StringType()), DoubleType())
      assertUdfForm(udf2, LongType(), LongType(), IntegerType())
      assertUdfForm(udf3, AnyType(), BooleanType(), TensorType(base = DoubleType()))
      assertUdfForm(udf4, StringType(), ListType(BooleanType()), ListType(StringType(), isNullable = true), ListType(DoubleType()))
      assertUdfForm(udf5, DoubleType(), DoubleType(), FloatType(), DoubleType(), DoubleType(), StringType())
    }
  }

  private def assertUdfForm(udf: UserDefinedFunction, returnType: DataType, argTypes: DataType *): Unit = {
    assert(udf.returnType == returnType)
    assert(udf.inputs.length == argTypes.length)
    udf.inputs.zip(argTypes).foreach {
      case (inputType, argType) => assert(inputType == argType)
    }
  }
}
