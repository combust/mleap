package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}
import org.scalatest.FunSpec

class DecisionTreeClassifierSpec extends FunSpec {

  describe("#getSchema") {
    it("has the correct inputs and outputs with only prediction column") {
      val transformer = new DecisionTreeClassifier("transformer", "features", "prediction", None, None, null)
      assert(transformer.getSchema().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }

    it("has the correct inputs and outputs with prediction column as well as probabilityCol") {
      val transformer = new DecisionTreeClassifier("transformer", "features", "prediction", None, Some("probability"), null)
      assert(transformer.getSchema().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType()),
          StructField("probability", TensorType(DoubleType()))))
    }

    it("has the correct inputs and outputs with prediction column as well as rawPredictionCol") {
      val transformer = new DecisionTreeClassifier("transformer", "features", "prediction", Some("rawPrediction"), None, null)
      assert(transformer.getSchema().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType()),
          StructField("rawPrediction", TensorType(DoubleType()))))
    }

    it("has the correct inputs and outputs with prediction column as well as both rawPredictionCol and probabilityCol") {
      val transformer = new DecisionTreeClassifier("transformer", "features", "prediction", Some("rawPrediction"), Some("probability"), null)
      assert(transformer.getSchema().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType()),
          StructField("rawPrediction", TensorType(DoubleType())),
          StructField("probability", TensorType(DoubleType()))))
    }
  }
}
