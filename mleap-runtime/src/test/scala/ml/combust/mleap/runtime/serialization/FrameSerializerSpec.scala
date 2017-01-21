package ml.combust.mleap.runtime.serialization

import ml.combust.bundle.ByteString
import ml.combust.mleap.runtime.test.MyCustomObject
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, MleapContext, Row}
import ml.combust.mleap.runtime.types._
import ml.combust.mleap.tensor.Tensor
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 11/1/16.
  */
class FrameSerializerSpec extends FunSpec {
  val schema = StructType(StructField("features", TensorType(DoubleType())),
    StructField("name", StringType()),
    StructField("list_data", ListType(StringType())),
    StructField("nullable_double", DoubleType(true)),
    StructField("float", FloatType(false)),
    StructField("byte_tensor", TensorType(ByteType(false))),
    StructField("short_list", ListType(ShortType(false))),
    StructField("byte_string", ByteStringType()),
    StructField("nullable_string", StringType(true))).get
  val dataset = LocalDataset(Row(Tensor.denseVector(Array(20.0, 10.0, 5.0)),
    "hello", Seq("hello", "there"),
    Option(56.7d), 32.4f,
    Tensor.denseVector(Array[Byte](1, 2, 3, 4)),
    Seq[Short](99, 12, 45),
    ByteString(Array[Byte](32, 4, 55, 67)),
    None))
  val frame = LeapFrame(schema, dataset).withOutput("custom_object", "name")((name: String) => MyCustomObject(name)).get
  import MleapContext.defaultContext

  describe("with format ml.combust.mleap.json") {
    it("serializes the leap frame as JSON") {
      val bytes = frame.writer("ml.combust.mleap.json").toBytes().get
      val dFrame = FrameReader("ml.combust.mleap.json").fromBytes(bytes).get
      frame.writer("ml.combust.mleap.json")

      assert(dFrame.schema == frame.schema)
      assert(dFrame.dataset == frame.dataset)
    }

    describe("row serializer") {
      it("serializes rows as JSON") {
        val writer = frame.schema.rowWriter("ml.combust.mleap.json")
        val reader = frame.schema.rowReader("ml.combust.mleap.json")
        val row = frame.dataset(0)
        val bytes = writer.toBytes(row).get
        val dRow = reader.fromBytes(bytes).get

        assert(row == dRow)
      }
    }
  }

  describe("with format ml.combust.mleap.binary") {
    it("serializes the leap frame as binary") {
      val bytes = frame.writer("ml.combust.mleap.binary").toBytes().get
      val dFrame = FrameReader("ml.combust.mleap.binary").fromBytes(bytes).get

      assert(dFrame.schema == frame.schema)
      assert(dFrame.dataset == frame.dataset)
    }

    describe("row serializer") {
      it("serializes rows as binary") {
        val writer = frame.schema.rowWriter("ml.combust.mleap.binary")
        val reader = frame.schema.rowReader("ml.combust.mleap.binary")
        val row = frame.dataset(0)
        val bytes = writer.toBytes(row).get
        val dRow = reader.fromBytes(bytes).get

        assert(row == dRow)
      }
    }
  }
}
