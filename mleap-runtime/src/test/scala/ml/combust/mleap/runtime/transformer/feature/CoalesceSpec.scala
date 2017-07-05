package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.CoalesceModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 1/5/17.
  */
class CoalesceSpec extends FunSpec {
  val schema = StructType(StructField("test1", ScalarType.Double.asNullable),
    StructField("test2", ScalarType.Double.asNullable),
    StructField("test3", ScalarType.Double.asNullable),
    StructField("test4", ScalarType.Double)).get
  val dataset = LocalDataset(Seq(Row(None, None, Some(23.4), 56.7),
    Row(None, None, None, 34.4)))
  val frame = LeapFrame(schema, dataset)

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

    describe("#getFields") {
      it("has the correct inputs and outputs") {
        assert(coalesce.schema.fields ==
          Seq(StructField("test1", ScalarType.Double.asNullable),
            StructField("test2", ScalarType.Double.asNullable),
            StructField("test3", ScalarType.Double.asNullable),
            StructField("test_bucket", ScalarType.Double.asNullable)))
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

    describe("#getFields") {
      it("has the correct inputs and outputs") {
        assert(coalesce.schema.fields ==
          Seq(StructField("test1", ScalarType.Double.asNullable),
            StructField("test3", ScalarType.Double.asNullable),
            StructField("test4", ScalarType.Double),
            StructField("test_bucket", ScalarType.Double.asNullable)))
      }
    }
  }
}
