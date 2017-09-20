package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ImputerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 1/4/17.
  */
class ImputerSpec extends FunSpec {
  describe("imputer") {
    val transformer = Imputer(
      shape = NodeShape().withStandardInput("test_a").
              withStandardOutput("test_out"),
      model = ImputerModel(45.7, 23.6, ""))

    describe("null values") {
      val schema = StructType(StructField("test_a", ScalarType.Double)).get
      val dataset = LocalDataset(Seq(Row(42.0), Row(null), Row(23.6)))
      val frame = LeapFrame(schema, dataset)

      it("transforms the leap frame using the given input and operation") {
        val data = transformer.transform(frame).get.dataset

        assert(data(0).getDouble(1) == 42.0)
        assert(data(1).getDouble(1) == 45.7)
        assert(data(2).getDouble(1) == 45.7)
      }

      it("has the correct inputs and outputs") {
        assert(transformer.schema.fields ==
          Seq(StructField("test_a", ScalarType.Double),
            StructField("test_out", ScalarType.Double)))
      }
    }

    describe("non-nullable columns") {
      val schema = StructType(StructField("test_a", ScalarType.Double)).get
      val dataset = LocalDataset(Seq(Row(42.0), Row(23.6), Row(Double.NaN)))
      val frame = LeapFrame(schema, dataset)
      val transformer2 = transformer.copy(shape = NodeShape().
              withStandardInput("test_a").
        withStandardOutput("test_out"),
        model = transformer.model.copy(nullableInput = false))

      it("transforms the leap frame using the given input and operation") {
        val data = transformer2.transform(frame).get.dataset

        assert(data(0).getDouble(1) == 42.0)
        assert(data(1).getDouble(1) == 45.7)
        assert(data(2).getDouble(1) == 45.7)
      }

      it("has the correct inputs and outputs") {
        assert(transformer2.schema.fields ==
          Seq(StructField("test_a", ScalarType.Double.nonNullable),
            StructField("test_out", ScalarType.Double)))
      }
    }
  }
}
