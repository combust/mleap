package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.IDFModel
import ml.combust.mleap.core.types._
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

class IDFSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = IDF(shape = NodeShape.vector(3, 3),
        model = IDFModel(Vectors.dense(Array(1.0, 2.0, 3.0))))
      assert(transformer.schema.fields ==
        Seq(StructField("input", TensorType.Double()),
          StructField("output", TensorType.Double())))
    }
  }
}