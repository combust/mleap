package ml.combust.mleap.core.linalg

import ml.combust.mleap.core.annotation.SparkCode
import org.apache.spark.ml.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.ml.linalg.mleap.{BLAS, VectorWithNorm}

/**
  * Created by hollinwilkins on 9/30/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/mllib/util/MLUtils.scala")
object LinalgUtils {
  lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }

  def fastSquaredDistance(v1: VectorWithNorm,
                                  v2: VectorWithNorm): Double = {
    fastSquaredDistance(v1.vector, v1.norm, v2.vector, v2.norm)
  }

  def fastSquaredDistance(v1: Vector,
                          norm1: Double,
                          v2: Vector,
                          norm2: Double,
                          precision: Double = 1e-6): Double = {
    val n = v1.size
    require(v2.size == n)
    require(norm1 >= 0.0 && norm2 >= 0.0)
    val sumSquaredNorm = norm1 * norm1 + norm2 * norm2
    val normDiff = norm1 - norm2
    var sqDist = 0.0
    /*
     * The relative error is
     * <pre>
     * EPSILON * ( \|a\|_2^2 + \|b\\_2^2 + 2 |a^T b|) / ( \|a - b\|_2^2 ),
     * </pre>
     * which is bounded by
     * <pre>
     * 2.0 * EPSILON * ( \|a\|_2^2 + \|b\|_2^2 ) / ( (\|a\|_2 - \|b\|_2)^2 ).
     * </pre>
     * The bound doesn't need the inner product, so we can use it as a sufficient condition to
     * check quickly whether the inner product approach is accurate.
     */
    val precisionBound1 = 2.0 * EPSILON * sumSquaredNorm / (normDiff * normDiff + EPSILON)
    if (precisionBound1 < precision) {
      sqDist = sumSquaredNorm - 2.0 * BLAS.dot(v1, v2)
    } else if (v1.isInstanceOf[SparseVector] || v2.isInstanceOf[SparseVector]) {
      val dotValue = BLAS.dot(v1, v2)
      sqDist = math.max(sumSquaredNorm - 2.0 * dotValue, 0.0)
      val precisionBound2 = EPSILON * (sumSquaredNorm + 2.0 * math.abs(dotValue)) /
        (sqDist + EPSILON)
      if (precisionBound2 > precision) {
        sqDist = Vectors.sqdist(v1, v2)
      }
    } else {
      sqDist = Vectors.sqdist(v1, v2)
    }
    sqDist
  }
}
