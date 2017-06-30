package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.CoalesceModel
import ml.combust.mleap.core.types.{DoubleType, StructField, StructType}
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import ml.combust.mleap.runtime.types._
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 1/5/17.
  */
class CoalesceSpec extends FunSpec {
  val schema = StructType(StructField("test1", DoubleType(true)),
    StructField("test2", DoubleType(true)),
    StructField("test3", DoubleType(true)),
    StructField("test4", DoubleType(false))).get
  val dataset = LocalDataset(Seq(Row(None, None, Some(23.4), 56.7),
    Row(None, None, None, 34.4)))
  val frame = LeapFrame(schema, dataset)

  describe("with all optional doubles") {
    val coalesce = Coalesce(inputCols = Array("test1", "test2", "test3"),
      inputDataTypes = Some(Array(DoubleType(true), DoubleType(true), DoubleType(true))),
      outputCol = "test_bucket",
      model = CoalesceModel())

    describe("#transform") {
      it("returns the non-null value or null if no value exists") {
        val data = coalesce.transform(frame).get.dataset

        assert(data(0).optionDouble(4).exists(_ == 23.4))
        assert(data(1).optionDouble(4).isEmpty)
      }
    }

    describe("#getFields") {
      it("has the correct inputs and outputs") {
        assert(coalesce.getFields().get ==
          Seq(StructField("test1", DoubleType(true)),
          StructField("test2", DoubleType(true)),
          StructField("test3", DoubleType(true)),
          StructField("test_bucket", DoubleType(true))))
      }
    }
  }

  describe("with a non-optional double") {
    val coalesce = Coalesce(inputCols = Array("test1", "test3", "test4"),
      inputDataTypes = Some(Array(DoubleType(true), DoubleType(true), DoubleType(false))),
      outputCol = "test_bucket",
      model = CoalesceModel())

    describe("#transform") {
      it("returns the first non-null value") {
        val data = coalesce.transform(frame).get.dataset

        assert(data(0).optionDouble(4).exists(_ == 23.4))
        assert(data(1).optionDouble(4).exists(_ == 34.4))
      }
    }

    describe("#getFields") {
      it("has the correct inputs and outputs") {
        assert(coalesce.getFields().get ==
          Seq(StructField("test1", DoubleType(true)),
            StructField("test3", DoubleType(true)),
            StructField("test4", DoubleType()),
          StructField("test_bucket", DoubleType(true))))
      }
    }
  }
}
