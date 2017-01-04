package ml.combust.mleap.core.feature

import ml.combust.mleap.core.annotation.SparkCode
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}

/**
  * Created by hollinwilkins on 12/28/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/feature/VectorIndexer.scala")
case class VectorIndexerModel(numFeatures: Int,
                              categoryMaps: Map[Int, Map[Double, Int]]) {
  val sortedCatFeatureIndices = categoryMaps.keys.toArray.sorted
  val localVectorMap = categoryMaps
  val localNumFeatures = numFeatures

  def apply(features: Vector): Vector = predict(features)
  def predict(features: Vector): Vector = {
    assert(features.size == localNumFeatures, "VectorIndexerModel expected vector of length" +
      s" $numFeatures but found length ${features.size}")
    features match {
      case dv: DenseVector =>
        val tmpv = dv.copy
        localVectorMap.foreach { case (featureIndex: Int, categoryMap: Map[Double, Int]) =>
          tmpv.values(featureIndex) = categoryMap(tmpv(featureIndex))
        }
        tmpv
      case sv: SparseVector =>
        // We use the fact that categorical value 0 is always mapped to index 0.
        val tmpv = sv.copy
        var catFeatureIdx = 0 // index into sortedCatFeatureIndices
      var k = 0 // index into non-zero elements of sparse vector
        while (catFeatureIdx < sortedCatFeatureIndices.length && k < tmpv.indices.length) {
          val featureIndex = sortedCatFeatureIndices(catFeatureIdx)
          if (featureIndex < tmpv.indices(k)) {
            catFeatureIdx += 1
          } else if (featureIndex > tmpv.indices(k)) {
            k += 1
          } else {
            tmpv.values(k) = localVectorMap(featureIndex)(tmpv.values(k))
            catFeatureIdx += 1
            k += 1
          }
        }
        tmpv
    }
  }
}
