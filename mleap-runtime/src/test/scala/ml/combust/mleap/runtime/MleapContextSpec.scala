package ml.combust.mleap.runtime

import ml.combust.mleap.runtime.test.MyCustomObject
import ml.combust.mleap.runtime.types.{StringType, StructField, StructType, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/31/16.
  */
class MleapContextSpec extends FunSpec {
  val context = MleapContext()
  val schema = StructType(Seq(StructField("features", TensorType.doubleVector()),
    StructField("name", StringType))).get
  val dataset = LocalDataset(Seq(Row(Vectors.dense(Array(20.0, 10.0, 5.0)), "hello")))
  val frame = LeapFrame(schema, dataset).withOutput("custom_object", "name")((name: String) => MyCustomObject(name)).get

  describe("with format ml.combust.mleap.json") {
    it("serializes the leap frame as JSON") {
      val bytes = context.serializer("ml.combust.mleap.json").toBytes(frame)
      val dFrame = context.serializer("ml.combust.mleap.json").fromBytes(bytes)

      assert(dFrame.schema == frame.schema)
      assert(dFrame.dataset == frame.dataset)
    }

    describe("row serializer") {
      it("serializes rows as JSON") {
        val serializer = context.serializer("ml.combust.mleap.json").rowSerializer(frame.schema)
        val row = frame.dataset(0)
        val bytes = serializer.toBytes(row)
        val dRow = serializer.fromBytes(bytes)

        assert(row == dRow)
      }
    }
  }

  describe("with format format ml.combust.mleap.binary") {
    it("serializes the leap frame as JSON") {
      val bytes = context.serializer("ml.combust.mleap.binary").toBytes(frame)
      val dFrame = context.serializer("ml.combust.mleap.binary").fromBytes(bytes)

      assert(dFrame.schema == frame.schema)
      assert(dFrame.dataset == frame.dataset)
    }

    describe("row serializer") {
      it("serializes rows as JSON") {
        val serializer = context.serializer("ml.combust.mleap.binary").rowSerializer(frame.schema)
        val row = frame.dataset(0)
        val bytes = serializer.toBytes(row)
        val dRow = serializer.fromBytes(bytes)

        assert(row == dRow)
      }
    }
  }
}
