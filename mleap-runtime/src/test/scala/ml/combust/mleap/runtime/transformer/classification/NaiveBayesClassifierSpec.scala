package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.NaiveBayesModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class NaiveBayesClassifierSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = NaiveBayesClassifier(shape = NodeShape.probabilisticClassifier(3, 2),
        model = new NaiveBayesModel(3, 2, null, null, null))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with probability column") {
      val transformer = NaiveBayesClassifier(shape = NodeShape.probabilisticClassifier(3, 2, probabilityCol = Some("probability")),
        model = new NaiveBayesModel(3, 2, null, null, null))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("probability", TensorType.Double(2)),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with rawPrediction column") {
      val transformer = NaiveBayesClassifier(shape = NodeShape.probabilisticClassifier(3, 2, rawPredictionCol = Some("rp")),
        model = new NaiveBayesModel(3, 2, null, null, null))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("rp", TensorType.Double(2)),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with both probability and rawPrediction columns") {
      val transformer = NaiveBayesClassifier(shape = NodeShape.probabilisticClassifier(3, 2,
        rawPredictionCol = Some("rp"),
        probabilityCol = Some("probability")),
        model = new NaiveBayesModel(3, 2, null, null, null))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("rp", TensorType.Double(2)),
          StructField("probability", TensorType.Double(2)),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }
  }
}
