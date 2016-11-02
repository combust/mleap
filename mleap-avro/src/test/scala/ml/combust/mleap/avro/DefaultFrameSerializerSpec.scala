package ml.combust.mleap.avro

import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, MleapContext, Row}
import ml.combust.mleap.runtime.serialization.FrameSerializerContext
import ml.combust.mleap.runtime.types._
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/31/16.
  */
class DefaultFrameSerializerSpec extends FunSpec {
  val schema = StructType(Seq(StructField("test_double", DoubleType),
    StructField("test_string", StringType),
    StructField("test_vector", TensorType.doubleVector()))).get
  val row = Row(2.0, "hello", Vectors.dense(Array(0.1, 2.33, 4.5)))
  val dataset = LocalDataset(Seq(row))
  val frame = LeapFrame(schema, dataset)
  val serializer = DefaultFrameSerializer(FrameSerializerContext("ml.combust.mleap.avro")(MleapContext()))

  it("serializes/deserializes avro properly") {
    val bytes = serializer.toBytes(frame)
    val dFrame = serializer.fromBytes(bytes)

    assert(frame.schema == dFrame.schema)
  }

  describe("row serializer") {
    val rowSerializer = serializer.rowSerializer(schema)

    it("serializes/deserializes avro rows properly") {
      val row = Row(2.0, "hello", Vectors.dense(Array(0.1, 2.33, 4.5)))
      val bytes = rowSerializer.toBytes(row)
      val dRow = rowSerializer.fromBytes(bytes)

      assert(row.toSeq == dRow.toSeq)
    }
  }

  describe("with an MleapContext") {
    it("serializes/deserializes avro properly") {
      val serializer = MleapContext.defaultContext.serializer("ml.combust.mleap.avro")
      val bytes = serializer.toBytes(frame)
      val dFrame = serializer.fromBytes(bytes)

      assert(frame == dFrame)
    }
  }
}
