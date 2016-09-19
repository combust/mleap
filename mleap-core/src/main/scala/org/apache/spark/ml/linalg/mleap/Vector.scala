package org.apache.spark.ml.linalg.mleap

import breeze.linalg.{Vector => BreezeVector, DenseVector, SparseVector}
import org.apache.spark.ml.linalg.{Vectors, Vector}

/**
  * Created by mikhail on 9/18/16.
  */
object Vector {
  implicit class VectorOps(vector: Vector) {
    def toBreeze: BreezeVector[Double] = vector.asBreeze
  }
  def fromBreeze(breezeVector: BreezeVector[Double]): Vector = Vectors.fromBreeze(breezeVector)

}