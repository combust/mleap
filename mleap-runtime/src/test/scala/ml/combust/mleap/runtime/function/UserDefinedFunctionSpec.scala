package ml.combust.mleap.runtime.function

import ml.combust.mleap.runtime.types._
import org.apache.spark.ml.linalg.Vector
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/21/16.
  */
class UserDefinedFunctionSpec extends FunSpec {
  describe("#apply") {
    it("creates the udf") {
      val udf0: UserDefinedFunction = () => "hello"
      val udf1: UserDefinedFunction = (v1: Double) => Array("hello")
      val udf2: UserDefinedFunction = (v1: Long, v2: Int) => v1 + v2
      val udf3: UserDefinedFunction = (v1: Boolean, v2: Vector) => "hello": Any
      val udf4: UserDefinedFunction = (v1: Array[Boolean], v2: Array[String], v3: Array[Double]) => "hello"
      val udf5: UserDefinedFunction = (v1: Double, v2: Double, v3: Double, v4: Double, v5: String) => 55d

      assertUdfForm(udf0, StringType)
      assertUdfForm(udf1, ListType(StringType), DoubleType)
      assertUdfForm(udf2, LongType, LongType, IntegerType)
      assertUdfForm(udf3, AnyType, BooleanType, TensorType.doubleVector())
      assertUdfForm(udf4, StringType, ListType(BooleanType), ListType(StringType), ListType(DoubleType))
      assertUdfForm(udf5, DoubleType, DoubleType, DoubleType, DoubleType, DoubleType, StringType)
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
