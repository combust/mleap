package ml.combust.mleap.core.feature

import org.apache.spark.ml.linalg.{DenseMatrix, Matrices, Vectors}
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/12/16.
  */
class PcaModelSpec extends FunSpec {
  describe("#apply") {
    it("uses the principal components matrix to transform a vector to a lower-dimensional vector") {
      val pc = new DenseMatrix(3, 2, Array[Double](1, -1, 2,
        0, -3, 1))
      val input = Vectors.dense(Array[Double](2, 1, 0))
      val pca = PcaModel(pc)

      assert(pca(input).toArray sameElements Array[Double](1, -3))
    }
  }
}