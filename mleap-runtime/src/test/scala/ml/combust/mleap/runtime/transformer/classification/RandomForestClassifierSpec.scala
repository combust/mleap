package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class RandomForestClassifierSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = RandomForestClassifier(shape = NodeShape.probabilisticClassifier(3, 2), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with probability column") {
      val transformer = RandomForestClassifier(shape = NodeShape.probabilisticClassifier(3, 2, probabilityCol = Some("probability")), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("probability", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with rawPrediction column") {
      val transformer = RandomForestClassifier(shape = NodeShape.probabilisticClassifier(3, 2, rawPredictionCol = Some("rp")), model = null)
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType(BasicType.Double, Seq(3))),
          StructField("rp", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with both probability and rawPrediction columns") {
      val transformer = RandomForestClassifier(shape = NodeShape.probabilisticClassifier(3, 2,
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
