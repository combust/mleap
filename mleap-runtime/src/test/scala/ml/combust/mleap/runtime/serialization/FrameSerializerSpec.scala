package ml.combust.mleap.runtime.serialization

import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, MleapContext, Row}
import ml.combust.mleap.tensor.{ByteString, Tensor}
import ml.combust.mleap.runtime.MleapSupport._
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 11/1/16.
  */
class FrameSerializerSpec extends FunSpec {
  val schema = StructType(StructField("features", TensorType(BasicType.Double)),
    StructField("name", ScalarType.String),
    StructField("list_data", ListType(BasicType.String)),
    StructField("nullable_double", ScalarType.Double.asNullable),
    StructField("float", ScalarType.Float),
    StructField("byte_tensor", TensorType(BasicType.Byte)),
    StructField("short_list", ListType(BasicType.Short)),
    StructField("byte_string", ScalarType.ByteString),
    StructField("nullable_string", ScalarType.String.asNullable)).get
  val dataset = LocalDataset(Row(Tensor.denseVector(Array(20.0, 10.0, 5.0)),
    "hello", Seq("hello", "there"),
    56.7d, 32.4f,
    Tensor.denseVector(Array[Byte](1, 2, 3, 4)),
    Seq[Short](99, 12, 45),
    ByteString(Array[Byte](32, 4, 55, 67)),
    null))
  val frame = LeapFrame(schema, dataset)
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
