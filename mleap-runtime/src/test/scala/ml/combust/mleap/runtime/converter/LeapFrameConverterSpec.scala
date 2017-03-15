package ml.combust.mleap.runtime.converter

import ml.combust.mleap.runtime.{DefaultLeapFrame, LocalDataset, MleapContext, Row}
import org.scalatest.FunSpec
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.test.MyCustomObject
import ml.combust.mleap.runtime.types.{DoubleType, StringType, StructField, StructType}

class LeapFrameConverterSpec extends FunSpec {

  describe("LeapFrameConverter") {
    val expectedSchema = StructType(Seq(StructField("test_string", StringType()),
      StructField("test_double", DoubleType()))).get
    val expectedCustomSchema = StructType(Seq(StructField("custom",
      MleapContext.defaultContext.customType[MyCustomObject]))).get

    val frameWith1Row = DefaultLeapFrame(expectedSchema,
      LocalDataset(Array(Row("hello", 42.13))))
    val frameWithMultipleRows = DefaultLeapFrame(expectedSchema,
      LocalDataset(Array(Row("hello", 42.13), Row("mleap", 4.3), Row("world", 1.2))))
    val frameWithCustomType = DefaultLeapFrame(expectedCustomSchema,
      LocalDataset(Array(Row(MyCustomObject("hello world")))))

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

    it("converts from a case class with custom type to a default leap frame") {
      assert(CustomData(MyCustomObject("hello world")).toLeapFrame == frameWithCustomType)
    }

    it("creates a Seq with one new instance of a case class with custom type from a default leap frame") {
      assert(frameWithCustomType.to[CustomData] == Seq(CustomData(MyCustomObject("hello world"))))
    }
  }
}

case class DummyData(test_string:String, test_double:Double)
case class CustomData(custom: MyCustomObject)
