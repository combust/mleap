package ml.combust.mleap.core.feature

import ml.combust.mleap.core.annotation.SparkCode
import org.apache.spark.ml.linalg.mleap.{VectorUtil => MleapVectors}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

import scala.math.{max, min}

/** Class for MaxAbs Scaler model.
  *
  * @param maxAbs max absolute value
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/feature/MaxAbsScaler.scala")
case class MaxAbsScalerModel(maxAbs: Vector)  extends Serializable {
  def apply(vector: Vector): Vector = {
    val maxAbsUnzero = Vectors.dense(maxAbs.toArray.map(x => if (x == 0) 1 else x))

    vector match {
      case DenseVector(values) =>
        val vs = values.clone()
        val size = vs.length
        var i = 0

        while (i < size) {
          if (!values(i).isNaN) {
            val rescale = max(-1.0, min(1.0, values(i) / maxAbsUnzero(i)))
            vs(i) = rescale
          }
          i += 1
        }
        Vectors.dense(vs)
      case SparseVector(size, indices, values) =>
        val vs = values.clone()
        val nnz = vs.length
        var i = 0
        while (i < nnz) {
          val raw = max(-1.0, min(1.0, values(i) / maxAbsUnzero(i)))

          vs(i) *= raw
          i += 1
        }
        Vectors.sparse(size, indices, vs)
    }
  }
}
