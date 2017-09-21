package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.MultiLayerPerceptronClassifierModel
import ml.combust.mleap.core.types._
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

class MultiLayerPerceptronClassifierSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer =
        MultiLayerPerceptronClassifier(shape = NodeShape.basicClassifier(3),
          model = new MultiLayerPerceptronClassifierModel(Seq(3, 1), Vectors.dense(Array(1.9, 2.2, 4, 1))))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }
  }
}