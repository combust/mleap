package ml.combust.mleap.core.classification

import ml.combust.mleap.core.types._
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.funspec.AnyFunSpec

class MultiLayerPerceptronClassifierModelSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("multi layer perceptron classifier model") {
    val model = new MultiLayerPerceptronClassifierModel(Seq(3, 1), Vectors.dense(Array(1.9, 2.2, 4, 1)))

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("features", TensorType.Double(3))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("raw_prediction", TensorType.Double(1)),
          StructField("probability", TensorType.Double(1)),
          StructField("prediction", ScalarType.Double.nonNullable)
        ))
    }
  }

}
