package org.apache.spark.ml.linalg.mleap

import org.apache.spark.ml.linalg

/** BLAS public interface to the private mllib-local BLAS.
  */
object BLAS {
  /** Calculate dot product of two vectors.
    *
    * @param v1 vector 1
    * @param v2 vector 2
    * @return dot product
    */
  def dot(v1: linalg.Vector, v2: linalg.Vector): Double = linalg.BLAS.dot(v1, v2)
}
