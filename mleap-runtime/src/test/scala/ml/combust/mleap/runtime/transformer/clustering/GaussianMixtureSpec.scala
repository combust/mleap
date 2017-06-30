package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.types.{DoubleType, IntegerType, StructField, TensorType}
import org.scalatest.FunSpec

class GaussianMixtureSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs with only prediction column") {
      val transformer = GaussianMixture("transformer", "features", "prediction", None, null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", IntegerType())))
    }

    it("has the correct inputs and outputs with only prediction column as well as probability column") {
      val transformer = GaussianMixture("transformer", "features", "prediction", Some("probability"), null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", IntegerType()),
          StructField("probability", TensorType(DoubleType()))))
    }
  }
}
