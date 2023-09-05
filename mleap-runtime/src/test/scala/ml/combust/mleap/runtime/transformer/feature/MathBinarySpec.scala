package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.BinaryOperation._
import ml.combust.mleap.core.feature.MathBinaryModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

/**
 * Created by hollinwilkins on 12/27/16.
 */
class MathBinarySpec extends org.scalatest.funspec.AnyFunSpec {
  val schema = StructType(StructField("test_a", ScalarType.Double), StructField("test_b", ScalarType.Double)).get
  val dataset = Seq(Row(5.6, 7.9))
  val frame = DefaultLeapFrame(schema, dataset)

  describe("with a and b inputs") {
    val transformer = MathBinary(
      shape = NodeShape().withInput("input_a", "test_a").
        withInput("input_b", "test_b").
        withStandardOutput("test_out"),
      model = MathBinaryModel(Divide, None, None))

    it("calculates the correct value") {
      val calc = transformer.transform(frame).get.dataset(0).getDouble(2)
      assert(calc == 5.6 / 7.9)
    }

    it("has correct inputs and outputs with a and b inputs") {
      assert(transformer.schema.fields ==
        Seq(StructField("test_a", ScalarType.Double.nonNullable),
          StructField("test_b", ScalarType.Double.nonNullable),
          StructField("test_out", ScalarType.Double.nonNullable)))
    }
  }

  describe("with a input") {
    val transformer = MathBinary(
      shape = NodeShape().withInput("input_a", "test_a").
        withStandardOutput("test_out"),
      model = MathBinaryModel(Multiply, None, Some(3.333)))

    it("calculates the value using the b default") {
      val calc = transformer.transform(frame).get.dataset(0).getDouble(2)
      assert(calc == 5.6 * 3.333)
    }

    it("has correct inputs and outputs using the default b") {
      assert(transformer.schema.fields ==
        Seq(StructField("test_a", ScalarType.Double.nonNullable),
          StructField("test_out", ScalarType.Double.nonNullable)))
    }
  }

  describe("with b input") {
    val transformer = MathBinary(
      shape = NodeShape().withInput("input_b", "test_b").
        withStandardOutput("test_out"),
      model = MathBinaryModel(Multiply, Some(3.333), None))

    it("calculates the value using the a default") {
      val calc = transformer.transform(frame).get.dataset(0).getDouble(2)
      assert(calc == 3.333 * 7.9)
    }

    it("has correct inputs and outputs using the default a") {
      assert(transformer.schema.fields ==
        Seq(StructField("test_b", ScalarType.Double.nonNullable),
          StructField("test_out", ScalarType.Double.nonNullable)))
    }
  }

  describe("with no inputs") {
    val transformer = MathBinary(shape = NodeShape().withStandardOutput("test_out"),
      model = MathBinaryModel(Multiply, Some(3.333), Some(5.223)))

    it("calculates the value using both defaults") {
      val calc = transformer.transform(frame).get.dataset(0).getDouble(2)
      assert(calc == 3.333 * 5.223)
    }

    it("has correct inputs and outputs using both defaults") {
      assert(transformer.schema.fields == Seq(StructField("test_out", ScalarType.Double.nonNullable)))
    }
  }
}
