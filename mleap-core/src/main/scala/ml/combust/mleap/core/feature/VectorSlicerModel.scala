package ml.combust.mleap.core.feature

import ml.combust.mleap.core.annotation.SparkCode
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.linalg.mleap.VectorUtil._

/**
  * Created by hollinwilkins on 12/28/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/feature/VectorSlicer.scala")
case class VectorSlicerModel(indices: Array[Int]) {
  def apply(features: Vector): Vector = features match {
    case features: DenseVector => Vectors.dense(indices.map(features.apply))
    case features: SparseVector => features.slice(indices)
  }
}
