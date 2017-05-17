package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.runtime.types.{DoubleType, StructField, TensorType}
import org.scalatest.FunSpec

class SupportVectorMachineSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = new SupportVectorMachine("transformer", "features", "prediction", None, None, null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }

    it("has the correct inputs and outputs with probability column") {
      val transformer = new SupportVectorMachine("transformer", "features", "prediction", None, Some("probability"), null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("probability", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }

    it("has the correct inputs and outputs with rawPrediction column") {
      val transformer = new SupportVectorMachine("transformer", "features", "prediction", Some("rawPrediction"), None, null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("rawPrediction", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }

    it("has the correct inputs and outputs with both probability and rawPrediction columns") {
      val transformer = new SupportVectorMachine("transformer", "features", "prediction", Some("rawPrediction"), Some("probability"), null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("rawPrediction", TensorType(DoubleType())),
          StructField("probability", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }
  }
}
