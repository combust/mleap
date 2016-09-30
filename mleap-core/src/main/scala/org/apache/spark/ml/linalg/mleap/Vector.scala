package org.apache.spark.ml.linalg.mleap

import org.apache.spark.ml.linalg

/**
  * Created by mikhail on 9/18/16.
  */
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