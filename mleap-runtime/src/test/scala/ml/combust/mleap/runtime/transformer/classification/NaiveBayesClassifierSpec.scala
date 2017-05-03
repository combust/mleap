package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}
import org.scalatest.FunSpec

class NaiveBayesClassifierSpec extends FunSpec {

  describe("#getSchema") {
    it("has the correct inputs and outputs") {
      val transformer = new NaiveBayesClassifier("transformer", "features", "prediction", None, None, null)
      assert(transformer.getSchema().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }

    it("has the correct inputs and outputs with probability column") {
      val transformer = new NaiveBayesClassifier("transformer", "features", "prediction", None, Some("probability"), null)
      assert(transformer.getSchema().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("probability", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }

    it("has the correct inputs and outputs with rawPrediction column") {
      val transformer = new NaiveBayesClassifier("transformer", "features", "prediction", Some("rawPrediction"), None, null)
      assert(transformer.getSchema().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("rawPrediction", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }

    it("has the correct inputs and outputs with both probability and rawPrediction columns") {
      val transformer = new NaiveBayesClassifier("transformer", "features", "prediction", Some("rawPrediction"), Some("probability"), null)
      assert(transformer.getSchema().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("rawPrediction", TensorType(DoubleType())),
          StructField("probability", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }
  }
}
