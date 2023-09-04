package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.MathUnaryModel
import ml.combust.mleap.core.feature.UnaryOperation.Sin
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

/**
 * Created by hollinwilkins on 12/27/16.
 */
class MathUnarySpec extends org.scalatest.funspec.AnyFunSpec {
  val schema = StructType(StructField("test_a", ScalarType.Double)).get
  val dataset = Seq(Row(42.0))
  val frame = DefaultLeapFrame(schema, dataset)

  val transformer = MathUnary(
    shape = NodeShape.feature(inputCol = "test_a",
      outputCol = "test_out"),
    model = MathUnaryModel(Sin))

  describe("#transform") {
    it("transforms the leap frame using the given input and operation") {
      val calc = transformer.transform(frame).get.dataset(0).getDouble(1)
      assert(calc == Math.sin(42.0))
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(transformer.schema.fields ==
        Seq(StructField("test_a", ScalarType.Double.nonNullable),
          StructField("test_out", ScalarType.Double.nonNullable)))
    }
  }
}