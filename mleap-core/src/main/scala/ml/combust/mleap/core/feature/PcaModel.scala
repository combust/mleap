package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import org.apache.spark.ml.linalg._

/** Class for principal components analysis model.
  *
  * @param k number of elements in output vector
  * @param principalComponents matrix of principal components
  */
case class PcaModel(principalComponents: DenseMatrix) extends Model {
  /** Convert input vector to its principal components.
    *
    * @param vector vector to transform
    * @return vector with only principal components
    */
  @SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/mllib/feature/PCA.scala")
  def apply(vector: Vector): Vector = vector match {
    case vector: DenseVector =>
      principalComponents.transpose.multiply(vector)
    case SparseVector(size, indices, values) =>
      val sm = Matrices.sparse(size, 1, Array(0, indices.length), indices, values).transpose
      val projection = sm.multiply(principalComponents)
      Vectors.dense(projection.values)
    case _ =>
      throw new IllegalArgumentException("Unsupported vector format. Expected " +
        s"SparseVector or DenseVector. Instead got: ${vector.getClass}")
  }
}
