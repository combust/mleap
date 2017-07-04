package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class DecisionTreeClassifierSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs with only prediction column") {
      val transformer = DecisionTreeClassifier(
        shape = NodeShape.probabilisticClassifier(3, 2), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with prediction column as well as probabilityCol") {
      val transformer = DecisionTreeClassifier(shape = NodeShape.probabilisticClassifier(3, 2, probabilityCol = Some("probability")), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("probability", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with prediction column as well as rawPredictionCol") {
      val transformer = DecisionTreeClassifier(shape = NodeShape.probabilisticClassifier(3, 2, rawPredictionCol = Some("rp")), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("rp", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with prediction column as well as both rawPredictionCol and probabilityCol") {
      val transformer = DecisionTreeClassifier(shape = NodeShape.probabilisticClassifier(3, 2,
        rawPredictionCol = Some("rp"),
        probabilityCol = Some("probability")), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("rp", TensorType(BasicType.Double, Seq(2))),
          StructField("probability", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double)))
    }
  }
}
