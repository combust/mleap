package org.apache.spark.ml.linalg.mleap

import ml.combust.mleap.core.annotation.SparkCode
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.{DenseVector, Matrix, Vector}

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

  def axpy(a: Double, X: linalg.DenseMatrix, Y: linalg.DenseMatrix): Unit = linalg.BLAS.axpy(a, X, Y)

  /**
    * y += a * x
    */
  def axpy(a: Double, x: linalg.Vector, y: linalg.Vector): Unit = linalg.BLAS.axpy(a, x, y)

  /**
    * x = a * x
    */
  def scal(a: Double, x: linalg.Vector): Unit = linalg.BLAS.scal(a, x)

  /**
    * y := alpha * A * x + beta * y
    * @param alpha a scalar to scale the multiplication A * x.
    * @param A the matrix A that will be left multiplied to x. Size of m x n.
    * @param x the vector x that will be left multiplied by A. Size of n x 1.
    * @param beta a scalar that can be used to scale vector y.
    * @param y the resulting vector y. Size of m x 1.
    */
  def gemv(alpha: Double,
           A: Matrix,
           x: Vector,
           beta: Double,
           y: DenseVector): Unit = linalg.BLAS.gemv(alpha, A, x, beta, y)
}
