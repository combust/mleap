package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.RandomForestClassifierModel
import ml.combust.mleap.core.types._

class RandomForestClassifierSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = RandomForestClassifier(shape = NodeShape.probabilisticClassifier(),
        model = new RandomForestClassifierModel(Seq(), Seq(), 3, 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with probability column") {
      val transformer = RandomForestClassifier(shape = NodeShape.probabilisticClassifier(probabilityCol = Some("probability")),
        model = new RandomForestClassifierModel(Seq(), Seq(), 3, 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("probability", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with rawPrediction column") {
      val transformer = RandomForestClassifier(shape = NodeShape.probabilisticClassifier(rawPredictionCol = Some("rp")),
        model = new RandomForestClassifierModel(Seq(), Seq(), 3, 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("rp", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with both probability and rawPrediction columns") {
      val transformer = RandomForestClassifier(shape = NodeShape.probabilisticClassifier(
        rawPredictionCol = Some("rp"),
        probabilityCol = Some("probability")),
        model = new RandomForestClassifierModel(Seq(), Seq(), 3, 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("rp", TensorType(BasicType.Double, Seq(2))),
          StructField("probability", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }
  }
}
