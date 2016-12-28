package org.apache.spark.ml.linalg.mleap

import ml.combust.mleap.core.annotation.SparkCode
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vector

/** BLAS public interface to the private mllib-local BLAS.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib-local/src/main/scala/org/apache/spark/ml/linalg/BLAS.scala")
object BLAS {
  /** Calculate dot product of two vectors.
    *
    * @param v1 vector 1
    * @param v2 vector 2
    * @return dot product
    */
  def dot(v1: linalg.Vector, v2: linalg.Vector): Double = linalg.BLAS.dot(v1, v2)

  /**
    * y += a * x
    */
  def axpy(a: Double, x: Vector, y: Vector): Unit = linalg.BLAS.axpy(a, x, y)

  /**
    * x = a * x
    */
  def scal(a: Double, x: Vector): Unit = linalg.BLAS.scal(a, x)
}
