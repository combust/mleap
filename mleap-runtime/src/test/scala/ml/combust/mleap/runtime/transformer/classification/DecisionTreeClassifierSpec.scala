package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.DecisionTreeClassifierModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class DecisionTreeClassifierSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs with only prediction column") {
      val transformer = DecisionTreeClassifier(
        shape = NodeShape.probabilisticClassifier(),
        model = new DecisionTreeClassifierModel(null, 3, 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with prediction column as well as probabilityCol") {
      val transformer = DecisionTreeClassifier(shape = NodeShape.probabilisticClassifier(probabilityCol = Some("probability")),
        model = new DecisionTreeClassifierModel(null, 3, 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("probability", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with prediction column as well as rawPredictionCol") {
      val transformer = DecisionTreeClassifier(shape = NodeShape.probabilisticClassifier(rawPredictionCol = Some("rp")),
        model = new DecisionTreeClassifierModel(null, 3, 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("rp", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with prediction column as well as both rawPredictionCol and probabilityCol") {
      val transformer = DecisionTreeClassifier(shape = NodeShape.probabilisticClassifier(rawPredictionCol = Some("rp"),
        probabilityCol = Some("probability")),
        model = new DecisionTreeClassifierModel(null, 3, 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("rp", TensorType(BasicType.Double, Seq(2))),
          StructField("probability", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }
  }
}
