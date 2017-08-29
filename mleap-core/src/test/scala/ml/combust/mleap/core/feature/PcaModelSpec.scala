package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{StructField, TensorType}
import org.apache.spark.ml.linalg.{DenseMatrix, Matrices, Vectors}
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/12/16.
  */
class PcaModelSpec extends FunSpec {
  describe("pca model") {
    val pc = new DenseMatrix(3, 2, Array[Double](1, -1, 2,
      0, -3, 1))
    val pca = PcaModel(pc)

    it("uses the principal components matrix to transform a vector to a lower-dimensional vector") {

      val input = Vectors.dense(Array[Double](2, 1, 0))

      assert(pca(input).toArray sameElements Array[Double](1, -3))
    }

    it("has the right input schema") {
      assert(pca.inputSchema.fields == Seq(StructField("input", TensorType.Double())))
    }

    it("has the right output schema") {
      assert(pca.outputSchema.fields == Seq(StructField("output", TensorType.Double())))
    }
  }
}