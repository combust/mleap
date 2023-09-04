package ml.combust.mleap.core.feature

import ml.combust.mleap.core.feature.BinaryOperation._
import ml.combust.mleap.core.types.{ScalarType, StructField}
import org.scalatest.funspec.AnyFunSpec

/**
  * Created by hollinwilkins on 12/27/16.
  */
class MathBinaryModelSpec extends org.scalatest.funspec.AnyFunSpec {
  def binaryLike(operation: BinaryOperation,
                 name: String,
                 a: Double,
                 b: Double,
                 expected: Double): Unit = {
    describe(operation.getClass.getSimpleName.dropRight(1)) {
      val model = MathBinaryModel(operation, None, None)
      it(s"has the name: $name") { assert(operation.name == name) }
      it("calculated the value properly") { assert(model(Some(a), Some(b)) == expected) }
      it("has the right input schema") {
        assert(model.inputSchema.fields == Seq(StructField("input_a" -> ScalarType.Double.nonNullable),
          StructField("input_b" -> ScalarType.Double.nonNullable)))
      }
      it("has the right output schema") {
        assert(model.outputSchema.fields == Seq(StructField("output" -> ScalarType.Double.nonNullable)))
      }
    }
  }

  binaryLike(Add, "add", 3.0, 6.7, 3.0 + 6.7)
  binaryLike(Subtract, "sub", 3.0, 6.7, 3.0 - 6.7)
  binaryLike(Multiply, "mul", 3.0, 6.7, 3.0 * 6.7)
  binaryLike(Divide, "div", 3.0, 6.7, 3.0 / 6.7)
  binaryLike(Remainder, "rem", 3.0, 6.7, 3.0 % 6.7)
  binaryLike(LogN, "log_n", 3.0, 6.7, Math.log(3.0) / Math.log(6.7))
  binaryLike(Pow, "pow", 3.0, 6.7, Math.pow(3.0, 6.7))
  binaryLike(Min, "min", 3.0, 6.7, Math.min(3.0, 6.7))
  binaryLike(Max, "max", 3.0, 6.7, Math.max(3.0, 6.7))
}
