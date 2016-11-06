package ml.combust.mleap.core.feature

import java.math.BigDecimal

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hwilkins on 1/21/16.
  */
class VectorAssemblerModelSpec extends FunSpec {
  describe("#apply") {
    it("assembles doubles and vectors into a new vector") {
      val assembler = VectorAssemblerModel.default
      val expectedArray = Array(45.0, 76.8, 23.0, 45.6, 0.0, 22.3, 45.6, 0.0, 99.3)

      assert(assembler(Array(45.0,
        new BigDecimal(76.8),
        Vectors.dense(Array(23.0, 45.6)),
        Vectors.sparse(5, Array(1, 2, 4), Array(22.3, 45.6, 99.3)))).toArray.sameElements(expectedArray))
    }
  }
}
