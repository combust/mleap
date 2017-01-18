package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.BinaryOperation._
import ml.combust.mleap.core.feature.MathBinaryModel
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import ml.combust.mleap.runtime.types.{DoubleType, StructField, StructType}
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 12/27/16.
  */
class MathBinarySpec extends FunSpec {
  val schema = StructType(StructField("test_a", DoubleType()), StructField("test_b", DoubleType())).get
  val dataset = LocalDataset(Seq(Row(5.6, 7.9)))
  val frame = LeapFrame(schema, dataset)

  describe("with a and b inputs") {
    val model = MathBinary(inputA = Some("test_a"),
      inputB = Some("test_b"),
      outputCol = "test_out",
      model = MathBinaryModel(Divide, None, None))

    it("calculates the correct value") {
      val calc = model.transform(frame).get.dataset(0).getDouble(2)
      assert(calc == 5.6 / 7.9)
    }
  }

  describe("with a input") {
    val model = MathBinary(inputA = Some("test_a"),
      outputCol = "test_out",
      model = MathBinaryModel(Multiply, None, Some(3.333)))

    it("calculates the value using the b default") {
      val calc = model.transform(frame).get.dataset(0).getDouble(2)
      assert(calc == 5.6 * 3.333)
    }
  }

  describe("with b input") {
    val model = MathBinary(inputB = Some("test_b"),
      outputCol = "test_out",
      model = MathBinaryModel(Multiply, Some(3.333), None))

    it("calculates the value using the a default") {
      val calc = model.transform(frame).get.dataset(0).getDouble(2)
      assert(calc == 3.333 * 7.9)
    }
  }

  describe("with no inputs") {
    val model = MathBinary(outputCol = "test_out",
      model = MathBinaryModel(Multiply, Some(3.333), Some(5.223)))

    it("calculates the value using the a default") {
      val calc = model.transform(frame).get.dataset(0).getDouble(2)
      assert(calc == 3.333 * 5.223)
    }
  }
}
