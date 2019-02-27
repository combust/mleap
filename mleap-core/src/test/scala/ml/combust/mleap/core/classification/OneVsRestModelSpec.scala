package ml.combust.mleap.core.classification

import ml.combust.mleap.core.types.{ScalarType, StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

class OneVsRestModelSpec extends FunSpec {

  describe("one vs rest model") {
    val model = new OneVsRestModel(Array(
      BinaryLogisticRegressionModel(Vectors.dense(1.0, 2.0), 0.7, 0.4)), 2)

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("features", TensorType.Double(2))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(
          StructField("probability", ScalarType.Double),
          StructField("raw_prediction", TensorType.Double(1)),
          StructField("prediction", ScalarType.Double)
        ))
    }
  }
}
