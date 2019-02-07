package ml.combust.mleap.core.feature

import ml.combust.mleap.core.feature.UnaryOperation._
import ml.combust.mleap.core.types.{ScalarType, StructField}
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 12/27/16.
  */
class MathUnaryModelSpec extends FunSpec {
  def unaryLike(operation: UnaryOperation,
                name: String,
                input: Double,
                expected: Double): Unit = {
    describe(operation.getClass.getSimpleName.dropRight(1)) {
      val model = MathUnaryModel(operation)

      it(s"has the name: $name") { assert(operation.name == name) }
      it("computes the value properly") { assert(model(input) == expected) }
      it("has the right input schema") {
        assert(model.inputSchema.fields == Seq(StructField("input", ScalarType.Double.nonNullable)))}
      it("has the right output schema") {
        assert(model.outputSchema.fields == Seq(StructField("output", ScalarType.Double.nonNullable)))}
    }
  }

  unaryLike(Log, "log", 65.3, Math.log(65.3))
  unaryLike(Exp, "exp", 42.0, Math.exp(42.0))
  unaryLike(Sqrt, "sqrt", 9.667, Math.sqrt(9.667))
  unaryLike(Sin, "sin", 45.2, Math.sin(45.2))
  unaryLike(Cos, "cos", 9982.2, Math.cos(9982.2))
  unaryLike(Tan, "tan", 88777.777, Math.tan(88777.777))
  unaryLike(Abs, "abs", -88777.777, Math.abs(-88777.777))
}
