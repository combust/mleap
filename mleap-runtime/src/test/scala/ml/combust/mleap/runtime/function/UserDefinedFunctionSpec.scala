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
      val udf3: UserDefinedFunction = (_: Boolean, _: Tensor[Double]) => "hello"
      val udf4: UserDefinedFunction = (_: Seq[Boolean], _: Option[Seq[String]], _: Seq[Double]) => "hello"
      val udf5: UserDefinedFunction = (_: Double, _: Float, _: Double, _: Double, _: String) => 55d

      assertUdfForm(udf0, ScalarType.String)
      assertUdfForm(udf1, ListType(BasicType.String), ScalarType.Double)
      assertUdfForm(udf2, ScalarType.Long, ScalarType.Long, ScalarType.Int)
      assertUdfForm(udf3, ScalarType.String, ScalarType.Boolean, TensorType(base = BasicType.Double))
      assertUdfForm(udf4, ScalarType.String, ListType(BasicType.Boolean), ListType(BasicType.String, isNullable = true), ListType(BasicType.Double))
      assertUdfForm(udf5, ScalarType.Double, ScalarType.Double, ScalarType.Float, ScalarType.Double, ScalarType.Double, ScalarType.String)
    }
  }

  private def assertUdfForm(udf: UserDefinedFunction, returnType: DataType, argTypes: DataType *): Unit = {
    assert(udf.outputs == Seq(returnType))
    assert(udf.inputs.length == argTypes.length)
    udf.inputs.zip(argTypes).foreach {
      case (inputType, argType) => assert(inputType == DataTypeSpec(argType))
    }
  }
}
