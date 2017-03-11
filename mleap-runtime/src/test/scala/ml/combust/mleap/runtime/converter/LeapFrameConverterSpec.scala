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

    val expectedFrameWith1Row = DefaultLeapFrame(expectedSchema,
      LocalDataset(Array(Row("hello", 42.13))))
    val expectedFrameWithMultipleRows = DefaultLeapFrame(expectedSchema,
      LocalDataset(Array(Row("hello", 42.13), Row("mleap", 4.3), Row("world", 1.2))))
    val expectedFrameWithCustomType = DefaultLeapFrame(expectedCustomSchema,
      LocalDataset(Array(Row(MyCustomObject("hello world")))))

    it("converts a case class to a default leap frame with 1 row") {
      assert(DummyData("hello", 42.13).toLeapFrame == expectedFrameWith1Row)
    }

    it("converts a case class to a default leap frame with multiple rows") {
      assert(Seq(DummyData("hello", 42.13), DummyData("mleap", 4.3),
        DummyData("world", 1.2)).toLeapFrame == expectedFrameWithMultipleRows)
    }

    it("converts a case class with custom type to a default leap frame") {
      assert(CustomData(MyCustomObject("hello world")).toLeapFrame == expectedFrameWithCustomType)
    }
  }
}

case class DummyData(test_string:String, test_double:Double)
case class CustomData(custom: MyCustomObject)
