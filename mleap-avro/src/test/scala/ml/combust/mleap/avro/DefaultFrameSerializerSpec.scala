package ml.combust.mleap.avro

import ml.combust.mleap.runtime.serialization._
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import ml.combust.mleap.runtime.types._
import ml.combust.mleap.tensor.Tensor
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/31/16.
  */
class DefaultFrameSerializerSpec extends FunSpec {
  val schema = StructType(StructField("test_double", DoubleType()),
    StructField("test_float", FloatType()),
    StructField("test_string", StringType()),
    StructField("test_vector", TensorType(DoubleType())),
    StructField("test_vector2", TensorType(DoubleType())),
    StructField("test_float_vector", TensorType(FloatType())),
    StructField("test_byte_vector", TensorType(ByteType())),
    StructField("test_short_vector", TensorType(ShortType())),
    StructField("test_nullable", StringType(true))).get
  val row = Row(2.0d, 45.3f, "hello",
    Tensor.denseVector(Array(0.1, 2.33, 4.5)),
    Tensor.denseVector(Array(0.1, 2.33, 4.5)),
    Tensor.denseVector(Array(0.1f, 2.33f, 4.5f)),
    Tensor.denseVector(Array[Byte](1, 2, 3, 4)),
    Tensor.denseVector(Array[Short](16, 45, 78)),
    None)
  val dataset = LocalDataset(Seq(row))
  val frame = LeapFrame(schema, dataset)

  import ml.combust.mleap.runtime.MleapContext.defaultContext

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
        val bytes = writer.toBytes(row)
        val dRow = reader.fromBytes(bytes)

        assert(row == dRow)
      }
    }
  }
}
