package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.SupportVectorMachineModel
import ml.combust.mleap.core.types._
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

class SupportVectorMachineSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = SupportVectorMachine(shape = NodeShape.probabilisticClassifier(),
        model = new SupportVectorMachineModel(Vectors.dense(1, 2, 3), 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with probability column") {
      val transformer = SupportVectorMachine(shape = NodeShape.probabilisticClassifier(probabilityCol = Some("probability")),
        model = new SupportVectorMachineModel(Vectors.dense(1, 2, 3), 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("probability", TensorType.Double(2)),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with rawPrediction column") {
      val transformer = SupportVectorMachine(shape = NodeShape.probabilisticClassifier(rawPredictionCol = Some("rp")),
        model = new SupportVectorMachineModel(Vectors.dense(1, 2, 3), 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("rp", TensorType(BasicType.Double, Seq(2))),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }

    it("has the correct inputs and outputs with both probability and rawPrediction columns") {
      val transformer = SupportVectorMachine(shape = NodeShape.probabilisticClassifier(
        rawPredictionCol = Some("rp"),
        probabilityCol = Some("probability")),
        model = new SupportVectorMachineModel(Vectors.dense(1, 2, 3), 2))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("rp", TensorType.Double(2)),
          StructField("probability", TensorType.Double(2)),
          StructField("prediction", ScalarType.Double.nonNullable)))
    }
  }
}
