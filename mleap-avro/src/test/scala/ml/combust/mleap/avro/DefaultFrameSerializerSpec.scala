package ml.combust.mleap.avro

import ml.combust.mleap.runtime.serialization._
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, MleapContext, Row}
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

  import ml.combust.mleap.runtime.MleapContext.defaultContext

  describe("with format ml.combust.mleap.avro") {
    it("serializes the leap frame as avro") {
      val bytes = FrameWriter("ml.combust.mleap.avro").toBytes(frame)
      val dFrame = FrameReader("ml.combust.mleap.avro").fromBytes(bytes)

      assert(dFrame.schema == frame.schema)
      assert(dFrame.dataset == frame.dataset)
    }

    describe("row serializer") {
      it("serializes rows as avro") {
        val writer = RowWriter(frame.schema, "ml.combust.mleap.avro")
        val reader = RowReader(frame.schema, "ml.combust.mleap.avro")
        val row = frame.dataset(0)
        val bytes = writer.toBytes(row)
        val dRow = reader.fromBytes(bytes)

        assert(row == dRow)
      }
    }
  }
}
