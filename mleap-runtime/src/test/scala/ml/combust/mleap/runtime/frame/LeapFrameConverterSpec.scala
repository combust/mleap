package ml.combust.mleap.runtime.frame

import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.MleapSupport._
import org.scalatest.FunSpec

class LeapFrameConverterSpec extends FunSpec {

  describe("LeapFrameConverter") {
    val expectedSchema = StructType(Seq(StructField("test_string", ScalarType.String),
      StructField("test_double", ScalarType.Double.nonNullable))).get

    val frameWith1Row = DefaultLeapFrame(expectedSchema,
      Seq(Row("hello", 42.13)))
    val frameWithMultipleRows = DefaultLeapFrame(expectedSchema,
      Seq(Row("hello", 42.13), Row("mleap", 4.3), Row("world", 1.2)))

    it("converts from a case class to a default leap frame with 1 row") {
      assert(DummyData("hello", 42.13).toLeapFrame == frameWith1Row)
    }

    it("creates a Seq with one new instance of a case class from a default leap frame with 1 row") {
      assert(frameWith1Row.to[DummyData] == Seq(DummyData("hello", 42.13)))
    }

    it("converts from a case class to a default leap frame with multiple rows") {
      assert(Seq(DummyData("hello", 42.13), DummyData("mleap", 4.3),
        DummyData("world", 1.2)).toLeapFrame == frameWithMultipleRows)
    }

    it("creates a Seq with multiple new instances of a case class from a default leap frame with multiple row") {
      assert(frameWithMultipleRows.to[DummyData] ==
        Seq(DummyData("hello", 42.13), DummyData("mleap", 4.3), DummyData("world", 1.2)))
    }
  }
}

case class DummyData(test_string: String, test_double: Double)
