package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.types.{DoubleType, StructField, TensorType}
import org.scalatest.FunSpec

class BisectingKMeansSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = new BisectingKMeans("transformer", "features", "prediction", null)
      assert(transformer.getFields().get ==
        Seq(StructField("features", TensorType(DoubleType())),
          StructField("prediction", DoubleType())))
    }
  }
}