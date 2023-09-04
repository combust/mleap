package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.PolynomialExpansionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.tensor.Tensor

/**
 * Created by mikhail on 10/16/16.
 */
class PolynomialExpansionSpec extends org.scalatest.funspec.AnyFunSpec {
  val schema = StructType(Seq(StructField("test_vec", TensorType(BasicType.Double)))).get
  val dataset = Seq(Row(Tensor.denseVector(Array(2.0, 3.0))))
  val frame = DefaultLeapFrame(schema, dataset)

  val transformer = PolynomialExpansion(
    shape = NodeShape.feature(inputCol = "test_vec", outputCol = "test_expanded"),
    model = PolynomialExpansionModel(2, 2))

  describe("#transform") {
    it("Takes 2+ dimensional vector and runs polynomial expansion") {
      val frame2 = transformer.transform(frame).get
      val data = frame2.dataset.toArray
      val expanded = data(0).getTensor[Double](1)
      val expectedVector = Array(2.0, 4.0, 3.0, 6.0, 9.0)

      assert(expanded.toArray.sameElements(expectedVector))
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(transformer.schema.fields ==
        Seq(StructField("test_vec", TensorType.Double(2)),
          StructField("test_expanded", TensorType.Double(5))))
    }
  }
}
