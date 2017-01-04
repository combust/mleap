package org.apache.spark.ml.linalg.mleap

import org.apache.spark.ml.linalg.Matrix

/**
  * Created by fshabbir on 12/25/16.
  */
object Matrices {
  def map(matrix: Matrix, f: (Double) => Double): Matrix = matrix.map(f)
}
