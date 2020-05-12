package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.{BucketizerModel, HandleInvalid}
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/28/16.
  */
class BucketizerSpec extends FunSpec {
  val schema = StructType(Seq(StructField("test_double", ScalarType.Double))).get
  val dataset = Seq(Row(11.0), Row(0.0), Row(55.0))
  val frame = DefaultLeapFrame(schema, dataset)

  val bucketizer = Bucketizer(
    shape = NodeShape.feature(inputCol = "test_double", outputCol = "test_bucket"),
    model = BucketizerModel(Array(0.0, 10.0, 20.0, 100.0)))

  describe("#transform") {
    it("places the input double into the appropriate bucket") {
      val frame2 = bucketizer.transform(frame).get
      val data = frame2.dataset.toArray

      assert(data(0).getDouble(1) == 1.0)
      assert(data(1).getDouble(1) == 0.0)
      assert(data(2).getDouble(1) == 2.0)
    }

    describe("with input feature out of range") {
      val dataset = Seq(Row(11.0), Row(0.0), Row(-23.0))
      val frame = DefaultLeapFrame(schema, dataset)

      it("returns a Failure") { assert(bucketizer.transform(frame).isFailure) }
    }

    describe("with invalid input column") {
      val bucketizer2 = bucketizer.copy(shape = NodeShape.feature(inputCol = "bad_double", outputCol = "test_bucket"))

      it("returns a Failure") { assert(bucketizer2.transform(frame).isFailure) }
    }

    describe("with skip logic") {
      val bucketizer2 = bucketizer.copy(model = bucketizer.model.copy(handleInvalid = HandleInvalid.Skip))

      val dataset = Seq(Row(11.0), Row(0.0), Row(Double.NaN))
      val frame = DefaultLeapFrame(schema, dataset)

      it("filters all of the invalid values") {
        val frame2 = bucketizer2.transform(frame).get

        val data = frame2.dataset
        assert(data.size == 2)
        assert(data(0).getDouble(1) == 1.0)
        assert(data(1).getDouble(1) == 0.0)
      }
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(bucketizer.schema.fields ==
        Seq(StructField("test_double", ScalarType.Double.nonNullable),
          StructField("test_bucket", ScalarType.Double.nonNullable)))
    }
  }
}
