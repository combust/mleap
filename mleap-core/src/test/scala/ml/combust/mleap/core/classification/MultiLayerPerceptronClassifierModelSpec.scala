package ml.combust.mleap.core.classification

import ml.combust.mleap.core.types._
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

class MultiLayerPerceptronClassifierModelSpec extends FunSpec {

  describe("multi layer perceptron classifier model") {
    val model = new MultiLayerPerceptronClassifierModel(Seq(3, 1), Vectors.dense(Array(1.9, 2.2, 4, 1)))

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3)))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("prediction", ScalarType.Double)))
    }
  }

}
