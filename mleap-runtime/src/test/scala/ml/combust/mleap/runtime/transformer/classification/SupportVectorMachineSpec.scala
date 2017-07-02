package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class SupportVectorMachineSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = new SupportVectorMachine("transformer", "features", "prediction", None, None, null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(BasicType.Double)),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with probability column") {
      val transformer = new SupportVectorMachine("transformer", "features", "prediction", None, Some("probability"), null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(BasicType.Double)),
          StructField("probability", TensorType(BasicType.Double)),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with rawPrediction column") {
      val transformer = new SupportVectorMachine("transformer", "features", "prediction", Some("rawPrediction"), None, null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(BasicType.Double)),
          StructField("rawPrediction", TensorType(BasicType.Double)),
          StructField("prediction", ScalarType.Double)))
    }

    it("has the correct inputs and outputs with both probability and rawPrediction columns") {
      val transformer = new SupportVectorMachine("transformer", "features", "prediction", Some("rawPrediction"), Some("probability"), null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(BasicType.Double)),
          StructField("rawPrediction", TensorType(BasicType.Double)),
          StructField("probability", TensorType(BasicType.Double)),
          StructField("prediction", ScalarType.Double)))
    }
  }
}
