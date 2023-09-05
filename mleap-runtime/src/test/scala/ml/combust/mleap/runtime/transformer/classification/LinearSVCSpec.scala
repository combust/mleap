package ml.combust.mleap.runtime.transformer.classification

import org.scalatest.funspec.AnyFunSpec
import ml.combust.mleap.core.types._
import org.apache.spark.ml.linalg.Vectors
import ml.combust.mleap.core.classification.LinearSVCModel

/**
  * Test spec for LinearSVC.
  * Note that Linear SVC is not a probabilistic classifier, so we use the basicClassifier() call.
  */
class LinearSVCSpec extends AnyFunSpec {
    describe("input/output schema")
    {
        it("has the correct inputs and outputs")
        {
            val transformer = LinearSVC(shape = NodeShape.basicClassifier(),
                model = new LinearSVCModel(Vectors.dense(1, 2, 3), 2))
            assert(transformer.schema.fields ==
                    Seq(StructField("features", TensorType.Double(3)),
                        StructField("prediction", ScalarType.Double.nonNullable)))
        }

        it("has the correct inputs and outputs with prediction column")
        {
            val transformer = LinearSVC(shape = NodeShape.probabilisticClassifier(rawPredictionCol = Some("rp"),predictionCol = "pred"),
                model = new LinearSVCModel(Vectors.dense(1, 2, 3), 2))
            assert(transformer.schema.fields ==
                    Seq(StructField("features", TensorType.Double(3)),
                        StructField("rp", TensorType.Double(2)),
                        StructField("pred", ScalarType.Double.nonNullable)))
        }

    }
}
