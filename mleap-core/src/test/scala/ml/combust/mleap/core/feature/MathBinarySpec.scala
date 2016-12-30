package ml.combust.mleap.core.feature

import ml.combust.mleap.core.feature.BinaryOperation._
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 12/27/16.
  */
class MathBinarySpec extends FunSpec {
  def binaryLike(operation: BinaryOperation,
                 name: String,
                 a: Double,
                 b: Double,
                 expected: Double): Unit = {
    describe(operation.getClass.getSimpleName.dropRight(1)) {
      val model = MathBinaryModel(operation, None, None)
      it(s"has the name: $name") { assert(operation.name == name) }
      it("calculated the value properly") { assert(model(Some(a), Some(b)) == expected) }
    }
  }

  binaryLike(Add, "add", 3.0, 6.7, 3.0 + 6.7)
  binaryLike(Subtract, "sub", 3.0, 6.7, 3.0 - 6.7)
  binaryLike(Multiply, "mul", 3.0, 6.7, 3.0 * 6.7)
  binaryLike(Divide, "div", 3.0, 6.7, 3.0 / 6.7)
  binaryLike(Remainder, "rem", 3.0, 6.7, 3.0 % 6.7)
  binaryLike(LogN, "log_n", 3.0, 6.7, Math.log(3.0) / Math.log(6.7))
  binaryLike(Pow, "pow", 3.0, 6.7, Math.pow(3.0, 6.7))
}
