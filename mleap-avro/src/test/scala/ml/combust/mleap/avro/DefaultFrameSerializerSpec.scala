package ml.combust.mleap.avro

import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.serialization.FrameReader
import ml.combust.mleap.tensor.{ByteString, Tensor}
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/31/16.
  */
class DefaultFrameSerializerSpec extends FunSpec {
  val schema = StructType(StructField("test_double", ScalarType.Double),
    StructField("test_float", ScalarType.Float),
    StructField("test_string", ScalarType.String),
    StructField("test_vector", TensorType(BasicType.Double)),
    StructField("test_vector2", TensorType(BasicType.Double)),
    StructField("test_float_vector", TensorType(BasicType.Float)),
    StructField("test_byte_vector", TensorType(BasicType.Byte)),
    StructField("test_short_vector", TensorType(BasicType.Short)),
    StructField("test_byte_string", ScalarType.ByteString),
    StructField("map_double", MapType(BasicType.String, BasicType.Double)),
    StructField("test_nullable", ScalarType.String.asNullable)).get
  val row = Row(2.0d, 45.3f, "hello",
    Tensor.denseVector(Array(0.1, 2.33, 4.5)),
    Tensor.denseVector(Array(0.1, 2.33, 4.5)),
    Tensor.denseVector(Array(0.1f, 2.33f, 4.5f)),
    Tensor.denseVector(Array[Byte](1, 2, 3, 4)),
    Tensor.denseVector(Array[Short](16, 45, 78)),
    ByteString(Array[Byte](1, 2, 3, 4, 5)),
    Map[String, Double]("foo" -> 42.0),
    null)
  val dataset = Seq(row)
  val frame = DefaultLeapFrame(schema, dataset)

  describe("with format ml.combust.mleap.avro") {
    it("serializes the leap frame as avro") {
      val bytes = frame.writer("ml.combust.mleap.avro").toBytes().get
      val dFrame = FrameReader("ml.combust.mleap.avro").fromBytes(bytes).get

      assert(dFrame.schema == frame.schema)
      assert(dFrame.dataset == frame.dataset)
    }

    describe("row serializer") {
      it("serializes rows as avro") {
        val writer = frame.schema.rowWriter("ml.combust.mleap.avro")
        val reader = frame.schema.rowReader("ml.combust.mleap.avro")
        val row = frame.dataset(0)
        val bytes = writer.toBytes(row).get
        val dRow = reader.fromBytes(bytes).get

        assert(row == dRow)
      }
    }
  }
}
