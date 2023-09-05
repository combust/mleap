package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.{BinaryLogisticRegressionModel, OneVsRestModel}
import ml.combust.mleap.core.types._
import org.apache.spark.ml.linalg.Vectors

class OneVsRestSpec extends org.scalatest.funspec.AnyFunSpec {
  describe("input/output schema") {
    it("has the correct inputs and outputs without probability column") {
      val transformer = OneVsRest(shape = NodeShape.basicClassifier(),
        model = new OneVsRestModel(Array(
          BinaryLogisticRegressionModel(Vectors.dense(1.0, 2.0), 0.7, 0.4)), 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(2)),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with probability column") {
      val transformer = OneVsRest(shape = NodeShape().withInput("features", "features").
        withOutput("probability", "prob").
        withOutput("prediction", "prediction"),
        model = new OneVsRestModel(Array(
          BinaryLogisticRegressionModel(Vectors.dense(1.0, 2.0), 0.7, 0.4)), 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(2)),
          StructField("prob", ScalarType.Double),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with raw prediction column") {
      val transformer = OneVsRest(shape = NodeShape().withInput("features", "features").
        withOutput("probability", "prob").
        withOutput("raw_prediction", "raw").
        withOutput("prediction", "prediction"),
        model = new OneVsRestModel(Array(
          BinaryLogisticRegressionModel(Vectors.dense(1.0, 2.0), 0.7, 0.4)), 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(2)),
          StructField("prob", ScalarType.Double),
          StructField("raw", TensorType.Double(1)),
          StructField("prediction", ScalarType.Double)))
    }
  }
}
