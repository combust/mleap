package ml.combust.mleap.runtime.function

import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.test.{MyCustomObject, MyCustomType}
import ml.combust.mleap.runtime.types._
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.linalg.Vector
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/21/16.
  */
class UserDefinedFunctionSpec extends FunSpec {
  describe("#apply") {
    it("creates the udf") {
      val udf0: UserDefinedFunction = () => "hello"
      val udf1: UserDefinedFunction = (v1: Double) => Seq("hello")
      val udf2: UserDefinedFunction = (v1: Long, v2: Int) => v1 + v2
      val udf3: UserDefinedFunction = (v1: Boolean, v2: Tensor[Double]) => "hello": Any
      val udf4: UserDefinedFunction = (v1: Seq[Boolean], v2: Option[Seq[String]], v3: Seq[Double]) => "hello"
      val udf5: UserDefinedFunction = (v1: Double, v2: Float, v3: Double, v4: Double, v5: String) => 55d
      val udf0custom: UserDefinedFunction = () => MyCustomObject("hello")

      assertUdfForm(udf0, StringType())
      assertUdfForm(udf1, ListType(StringType()), DoubleType())
      assertUdfForm(udf2, LongType(), LongType(), IntegerType())
      assertUdfForm(udf3, AnyType(), BooleanType(), TensorType(base = DoubleType()))
      assertUdfForm(udf4, StringType(), ListType(BooleanType()), ListType(StringType(), isNullable = true), ListType(DoubleType()))
      assertUdfForm(udf5, DoubleType(), DoubleType(), FloatType(), DoubleType(), DoubleType(), StringType())
      assertUdfForm(udf0custom, MleapContext.defaultContext.customType[MyCustomObject])
    }
  }

  private def assertUdfForm(udf: UserDefinedFunction, returnType: DataType, argTypes: DataType *): Unit = {
    assert(udf.returnTypes == returnType)
    assert(udf.inputs.length == argTypes.length)
    udf.inputs.zip(argTypes).foreach {
      case (inputType, argType) => assert(inputType == argType)
    }
  }
}
