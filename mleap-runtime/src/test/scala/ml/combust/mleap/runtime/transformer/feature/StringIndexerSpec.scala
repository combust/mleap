package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.{HandleInvalid, StringIndexerModel}
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, RowTransformer}

/**
 * Created by hollinwilkins on 9/15/16.
 */
class StringIndexerSpec extends org.scalatest.funspec.AnyFunSpec {
  val schema = StructType(Seq(StructField("test_string", ScalarType.String))).get
  val dataset = Seq(Row("index1"), Row("index2"), Row("index3"))
  val frame = DefaultLeapFrame(schema, dataset)

  val stringIndexer = StringIndexer(
    shape = NodeShape.feature(
      inputPort="input0",
      outputPort="output0",
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
      val stringIndexer2 = stringIndexer.copy(shape = NodeShape.feature(
        inputPort="input0",
        outputPort="output0",
        inputCol = "bad_input",
        outputCol = "bad_output"))

      it("returns a Failure") {
        assert(stringIndexer2.transform(frame).isFailure)
      }

    }

    describe("with invalid string") {
      val frame2 = frame.copy(dataset = Seq(Row("bad_index")))

      it("returns a Failure") {
        assert(stringIndexer.transform(frame2).isFailure)
      }
    }

    describe("with skip logic") {
      val stringIndexer2 = stringIndexer.copy(model = stringIndexer.model.copy(handleInvalid = HandleInvalid.Skip))
      val dataset = Seq(Row("index_bad"), Row("index2"), Row("index_bad"))
      val frame = DefaultLeapFrame(schema, dataset)

      it("filters all of the invalid labels") {
        val frame2 = stringIndexer2.transform(frame).get

        assert(frame2.dataset.size == 1)
        assert(frame2.dataset.head.getDouble(1) == 1.0)
      }

      describe("with a row transformer") {
        it("filters individual rows") {
          val rt = stringIndexer2.transform(RowTransformer(frame.schema)).get

          assert(rt.transform(Row("index_bad")) == null)
          assert(rt.transform(Row("index1")).getDouble(1) == 0.0)
          assert(rt.transform(Row("index2")).getDouble(1) == 1.0)
          assert(rt.transform(Row("index3")).getDouble(1) == 2.0)
        }
      }
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
