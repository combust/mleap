package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.funspec.AnyFunSpec

class IDFModelSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("idf model") {
    val model = IDFModel(Vectors.dense(Array(1.0, 2.0)))

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", TensorType.Double())))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", TensorType.Double())))
    }
  }
}
