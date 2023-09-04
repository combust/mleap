package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.CoalesceModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import org.scalatest.funspec.AnyFunSpec

/**
  * Created by hollinwilkins on 1/5/17.
  */
class CoalesceSpec extends AnyFunSpec {
  val schema = StructType(StructField("test1", ScalarType.Double),
    StructField("test2", ScalarType.Double),
    StructField("test3", ScalarType.Double),
    StructField("test4", ScalarType.Double.nonNullable)).get
  val dataset = Seq(Row(null, null, 23.4, 56.7),
    Row(null, null, null, 34.4))
  val frame = DefaultLeapFrame(schema, dataset)

  describe("with all optional doubles") {
    val coalesce = Coalesce(shape = NodeShape().withInput("input0", "test1").
              withInput("input1", "test2").
              withInput("input2", "test3").
          withStandardOutput("test_bucket"),
      model = CoalesceModel(Seq(true, true, true)))

    describe("#transform") {
      it("returns the non-null value or null if no value exists") {
        val data = coalesce.transform(frame).get.dataset

        assert(data(0).optionDouble(4).exists(_ == 23.4))
        assert(data(1).optionDouble(4).isEmpty)
      }
    }

    describe("input/output schema") {
      it("has the correct inputs and outputs") {
        assert(coalesce.schema.fields ==
          Seq(StructField("test1", ScalarType.Double),
            StructField("test2", ScalarType.Double),
            StructField("test3", ScalarType.Double),
            StructField("test_bucket", ScalarType.Double)))
      }
    }
  }

  describe("with a non-optional double") {
    val coalesce = Coalesce(shape = NodeShape().withInput("input0", "test1").
              withInput("input1", "test3").
              withInput("input2", "test4").
          withStandardOutput("test_bucket"),
      model = CoalesceModel(Seq(true, true, false)))

    describe("#transform") {
      it("returns the first non-null value") {
        val data = coalesce.transform(frame).get.dataset

        assert(data(0).optionDouble(4).exists(_ == 23.4))
        assert(data(1).optionDouble(4).exists(_ == 34.4))
      }
    }

    describe("input/output schema") {
      it("has the correct inputs and outputs") {
        assert(coalesce.schema.fields ==
          Seq(StructField("test1", ScalarType.Double),
            StructField("test3", ScalarType.Double),
            StructField("test4", ScalarType.Double.nonNullable),
            StructField("test_bucket", ScalarType.Double)))
      }
    }
  }
}
