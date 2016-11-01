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
  val dataset = LocalDataset(Seq(Row(2.0, "hello", Vectors.dense(Array(0.1, 2.33, 4.5)))))
  val frame = LeapFrame(schema, dataset)

  describe("serializing to avro") {
    it("does the thing") {
      val serializer = DefaultFrameSerializer(FrameSerializerContext("ml.combust.mleap.avro")(MleapContext()))
      val bytes = serializer.toBytes(frame)
      val dFrame = serializer.fromBytes(bytes)

      assert(frame.schema == dFrame.schema)
    }
  }
}
