package ml.combust.mleap.core.clustering

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/30/16.
  */
class KMeansModelSpec extends FunSpec {
  val v1 = Vectors.dense(Array(1.0, 2.0, 55.0))
  val v2 = Vectors.dense(Array(11.0, 200.0, 55.0))
  val v3 = Vectors.dense(Array(100.0, 22.0, 55.0))
  val km = KMeansModel(Array(v1, v2, v3), 3)

  describe("#apply") {
    it("finds the closest cluster") {
      assert(km(Vectors.dense(Array(2.0, 5.0, 34.0))) == 0)
      assert(km(Vectors.dense(Array(20.0, 230.0, 34.0))) == 1)
      assert(km(Vectors.dense(Array(111.0, 20.0, 56.0))) == 2)
    }
  }
}
