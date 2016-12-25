package ml.combust.mleap.runtime.serialization

import ml.combust.mleap.runtime.test.MyCustomObject
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, MleapContext, Row}
import ml.combust.mleap.runtime.types._
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 11/1/16.
  */
class FrameSerializerSpec extends FunSpec {
  val schema = StructType(Seq(StructField("features", TensorType.doubleVector()),
    StructField("name", StringType),
    StructField("list_data", ListType(StringType)))).get
  val dataset = LocalDataset(Seq(Row(Vectors.dense(Array(20.0, 10.0, 5.0)), "hello", Seq("hello", "there"))))
  val frame = LeapFrame(schema, dataset).withOutput("custom_object", "name")((name: String) => MyCustomObject(name)).get
  import MleapContext.defaultContext

  describe("with format ml.combust.mleap.json") {
    it("serializes the leap frame as JSON") {
      val bytes = FrameWriter("ml.combust.mleap.json").toBytes(frame)
      val dFrame = FrameReader("ml.combust.mleap.json").fromBytes(bytes)

      assert(dFrame.schema == frame.schema)
      assert(dFrame.dataset == frame.dataset)
    }

    describe("row serializer") {
      it("serializes rows as JSON") {
        val writer = RowWriter(frame.schema, "ml.combust.mleap.json")
        val reader = RowReader(frame.schema, "ml.combust.mleap.json")
        val row = frame.dataset(0)
        val bytes = writer.toBytes(row)
        val dRow = reader.fromBytes(bytes)

        assert(row == dRow)
      }
    }
  }

  describe("with format ml.combust.mleap.binary") {
    it("serializes the leap frame as binary") {
      val bytes = FrameWriter("ml.combust.mleap.binary").toBytes(frame)
      val dFrame = FrameReader("ml.combust.mleap.binary").fromBytes(bytes)

      assert(dFrame.schema == frame.schema)
      assert(dFrame.dataset == frame.dataset)
    }

    describe("row serializer") {
      it("serializes rows as binary") {
        val writer = RowWriter(frame.schema, "ml.combust.mleap.binary")
        val reader = RowReader(frame.schema, "ml.combust.mleap.binary")
        val row = frame.dataset(0)
        val bytes = writer.toBytes(row)
        val dRow = reader.fromBytes(bytes)

        assert(row == dRow)
      }
    }
  }
}
