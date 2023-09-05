package ml.combust.mleap.runtime.transformer.sklearn

import ml.combust.mleap.core.sklearn.PolynomialFeaturesModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.tensor.Tensor
import org.scalatest.funspec.AnyFunSpec

class PolynomialFeaturesSpec extends org.scalatest.funspec.AnyFunSpec {

  val transformer = PolynomialFeatures(shape = NodeShape.feature(
    inputCol = "test_vec",
    outputCol = "test_poly"),
    model = PolynomialFeaturesModel("[x0,x0^2]"))

  describe("sklearn polynomial features") {

    it("has the right input schema") {
      assert(transformer.inputSchema.fields ==
        Seq(StructField("test_vec", TensorType.Double(1))))
    }

    it("has the right output schema") {
      assert(transformer.outputSchema.fields ==
        Seq(StructField("test_poly", TensorType.Double(2))))
    }

    it ("calculates poly features based off given combinations") {

      val schema = StructType(Seq(StructField("test_vec", TensorType(BasicType.Double)))).get
      val dataset = Seq(Row(Tensor.denseVector(Array(2.0))))
      val frame = DefaultLeapFrame(schema, dataset)

      val frame2 = transformer.transform(frame).get
      val data = frame2.dataset(0).getTensor[Double](1)

      assert(data(0) == 2)
      assert(data(1) == 4)
    }
  }
}
