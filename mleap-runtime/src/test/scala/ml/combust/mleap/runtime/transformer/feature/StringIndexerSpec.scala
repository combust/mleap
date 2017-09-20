package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.StringIndexerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/15/16.
  */
class StringIndexerSpec extends FunSpec {
  val schema = StructType(Seq(StructField("test_string", ScalarType.String))).get
  val dataset = LocalDataset(Seq(Row("index1"), Row("index2"), Row("index3")))
  val frame = LeapFrame(schema, dataset)

  val stringIndexer = StringIndexer(
    shape = NodeShape.scalar(inputBase = BasicType.String,
      outputBase = BasicType.Double,
      inputCol = "test_string",
      outputCol = "test_index"),
    model = StringIndexerModel(Seq("index1", "index2", "index3")))

  describe("#transform") {
    it("converts input string into an index") {
      val frame2 = stringIndexer.transform(frame).get
      val data = frame2.dataset.toArray

      assert(data(0).getDouble(1) == 0.0)
      assert(data(1).getDouble(1) == 1.0)
      assert(data(2).getDouble(1) == 2.0)
    }

    describe("with invalid input column") {
      val stringIndexer2 = stringIndexer.copy(shape = NodeShape().withStandardInput("bad_input").
              withStandardOutput("output"))

      it("returns a Failure") { assert(stringIndexer2.transform(frame).isFailure) }
    }

    describe("with invalid string") {
      val frame2 = frame.copy(dataset = LocalDataset(Array(Row("bad_index"))))

      it("returns a Failure") { assert(stringIndexer.transform(frame2).isFailure) }
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(stringIndexer.schema.fields ==
        Seq(StructField("test_string", ScalarType.String),
          StructField("test_index", ScalarType.Double.nonNullable)))
    }
  }
}
