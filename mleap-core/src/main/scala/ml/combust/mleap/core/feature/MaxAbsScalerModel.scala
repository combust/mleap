package ml.combust.mleap.core.feature

import org.apache.spark.ml.linalg.mleap.{Vector => MleapVectors}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import scala.math.{max, min}

/** Class for MaxAbs Scaler
  * /Users/mikhail/combust/combust-mleap/mleap-core/src/test/scala/ml/combust/mleap/core/feature/MaxAbsScalerModel.scala
  * Created by mikhail on 9/18/16.
  */
case class MaxAbsScalerModel(
                         maxAbs: Vector
                       )  extends Serializable{
  def apply(vector: Vector): Vector = {
    println("Here")
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
