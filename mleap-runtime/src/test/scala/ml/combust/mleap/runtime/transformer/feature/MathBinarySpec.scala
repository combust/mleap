package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.BinaryOperation._
import ml.combust.mleap.core.feature.MathBinaryModel
import ml.combust.mleap.runtime.types.{DoubleType, StructField, StructType}
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 12/27/16.
  */
class MathBinarySpec extends FunSpec {
  val schema = StructType(StructField("test_a", DoubleType()), StructField("test_b", DoubleType())).get
  val dataset = LocalDataset(Seq(Row(5.6, 7.9)))
  val frame = LeapFrame(schema, dataset)

  describe("with a and b inputs") {
    val transformer = MathBinary(inputA = Some("test_a"),
      inputB = Some("test_b"),
      outputCol = "test_out",
      model = MathBinaryModel(Divide, None, None))

    it("calculates the correct value") {
      val calc = transformer.transform(frame).get.dataset(0).getDouble(2)
      assert(calc == 5.6 / 7.9)
    }

    it("has correct inputs and outputs with a and b inputs") {
      assert(transformer.getFields().get ==
        Seq(StructField("test_a", DoubleType()),
          StructField("test_b", DoubleType()),
          StructField("test_out", DoubleType())))
    }
  }

  describe("with a input") {
    val transformer = MathBinary(inputA = Some("test_a"),
      outputCol = "test_out",
      model = MathBinaryModel(Multiply, None, Some(3.333)))

    it("calculates the value using the b default") {
      val calc = transformer.transform(frame).get.dataset(0).getDouble(2)
      assert(calc == 5.6 * 3.333)
    }

    it("has correct inputs and outputs using the default b") {
      assert(transformer.getFields().get ==
        Seq(StructField("test_a", DoubleType()),
          StructField("test_out", DoubleType())))
    }
  }

  describe("with b input") {
    val transformer = MathBinary(inputB = Some("test_b"),
      outputCol = "test_out",
      model = MathBinaryModel(Multiply, Some(3.333), None))

    it("calculates the value using the a default") {
      val calc = transformer.transform(frame).get.dataset(0).getDouble(2)
      assert(calc == 3.333 * 7.9)
    }

    it("has correct inputs and outputs using the default a") {
      assert(transformer.getFields().get ==
        Seq(StructField("test_b", DoubleType()),
          StructField("test_out", DoubleType())))
    }
  }

  describe("with no inputs") {
    val transformer = MathBinary(outputCol = "test_out",
      model = MathBinaryModel(Multiply, Some(3.333), Some(5.223)))

    it("calculates the value using both defaults") {
      val calc = transformer.transform(frame).get.dataset(0).getDouble(2)
      assert(calc == 3.333 * 5.223)
    }

    it("has correct inputs and outputs using both defaults") {
      assert(transformer.getFields().get == Seq(StructField("test_out", DoubleType())))
    }
  }
}
