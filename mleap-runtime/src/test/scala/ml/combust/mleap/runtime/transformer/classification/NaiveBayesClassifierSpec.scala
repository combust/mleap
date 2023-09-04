package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.NaiveBayesModel
import ml.combust.mleap.core.types._

class NaiveBayesClassifierSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = NaiveBayesClassifier(shape = NodeShape.probabilisticClassifier(),
        model = new NaiveBayesModel(3, 2, null, null, null))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with probability column") {
      val transformer = NaiveBayesClassifier(shape = NodeShape.probabilisticClassifier(probabilityCol = Some("probability")),
        model = new NaiveBayesModel(3, 2, null, null, null))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("probability", TensorType.Double(2)),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with rawPrediction column") {
      val transformer = NaiveBayesClassifier(shape = NodeShape.probabilisticClassifier(rawPredictionCol = Some("rp")),
        model = new NaiveBayesModel(3, 2, null, null, null))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("rp", TensorType.Double(2)),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with both probability and rawPrediction columns") {
      val transformer = NaiveBayesClassifier(shape = NodeShape.probabilisticClassifier(
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
