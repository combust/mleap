package org.apache.spark.ml.linalg.mleap

import ml.combust.mleap.core.annotation.SparkCode
import org.apache.spark.ml.linalg

/** Expose private methods from mllib local.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib-local/src/main/scala/org/apache/spark/ml/linalg/Vector.scala")
object Vector {
  implicit class VectorOps(vector: linalg.Vector) {
    def toBreeze: breeze.linalg.Vector[Double] = vector.asBreeze
  }
  def fromBreeze(breezeVector: breeze.linalg.Vector[Double]): linalg.Vector = linalg.Vectors.fromBreeze(breezeVector)

}

object VectorWithNorm {
  def apply(vector: linalg.Vector): VectorWithNorm = {
    VectorWithNorm(vector, linalg.Vectors.norm(vector, 2.0))
  }
}
case class VectorWithNorm(vector: linalg.Vector, norm: Double)