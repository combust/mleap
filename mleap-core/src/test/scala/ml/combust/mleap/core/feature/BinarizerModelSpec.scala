package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types._
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by fshabbir on 12/1/16.
  */
class BinarizerModelSpec extends FunSpec {
  describe("binarizer with several inputs"){
    val binarizer = BinarizerModel(0.3, TensorShape(3))

    it("Makes a value 0 or 1 based on the threshold") {
      val features = Vectors.dense(Array(0.1, 0.4, 0.3))
      val binFeatures = binarizer(features).toArray

      assert(binFeatures(0) == 0.0)
      assert(binFeatures(1) == 1.0)
      assert(binFeatures(2) == 0.0)
    }

    it("Has the right input schema") {
      assert(binarizer.inputSchema.fields ==
        Seq(StructField("input", TensorType.Double(3))))
    }

    it("Has the right output schema") {
      assert(binarizer.outputSchema.fields ==
        Seq(StructField("output", TensorType.Double(3))))
    }
  }

  describe("binarizer with one input") {
    val binarizer = BinarizerModel(0.3, ScalarShape())

    it("Has the right input schema") {
      assert(binarizer.inputSchema.fields ==
        Seq(StructField("input", ScalarType.Double.nonNullable)))
    }

    it("Has the right output schema") {
      assert(binarizer.outputSchema.fields ==
        Seq(StructField("output", ScalarType.Double.nonNullable)))
    }
  }
}
