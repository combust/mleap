package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class DecisionTreeClassifierSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs with only prediction column") {
      val transformer = new DecisionTreeClassifier("transformer", "features", "prediction", None, None, null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(BasicType.Double)),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with prediction column as well as probabilityCol") {
      val transformer = new DecisionTreeClassifier("transformer", "features", "prediction", None, Some("probability"), null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(BasicType.Double)),
          StructField("probability", TensorType(BasicType.Double)),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with prediction column as well as rawPredictionCol") {
      val transformer = new DecisionTreeClassifier("transformer", "features", "prediction", Some("rawPrediction"), None, null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(BasicType.Double)),
          StructField("rawPrediction", TensorType(BasicType.Double)),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with prediction column as well as both rawPredictionCol and probabilityCol") {
      val transformer = new DecisionTreeClassifier("transformer", "features", "prediction", Some("rawPrediction"), Some("probability"), null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(BasicType.Double)),
          StructField("rawPrediction", TensorType(BasicType.Double)),
          StructField("probability", TensorType(BasicType.Double)),
          StructField("prediction", ScalarType.Double)))
    }
  }
}
