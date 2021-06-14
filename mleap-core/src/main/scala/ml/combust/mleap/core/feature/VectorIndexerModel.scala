package ml.combust.mleap.core.feature

import java.util.NoSuchElementException

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{StructType, TensorType}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}

/**
  * Created by hollinwilkins on 12/28/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.4.5/mllib/src/main/scala/org/apache/spark/ml/feature/VectorIndexer.scala")
case class VectorIndexerModel(numFeatures: Int,
                              categoryMaps: Map[Int, Map[Double, Int]],
                              handleInvalid: HandleInvalid = HandleInvalid.Error) extends Model {
  val sortedCatFeatureIndices = categoryMaps.keys.toArray.sorted
  val localVectorMap = categoryMaps
  val localNumFeatures = numFeatures
  val localHandleInvalid = handleInvalid

  def apply(features: Vector): Vector = predict(features)
  def predict(features: Vector): Vector = {
    assert(features.size == localNumFeatures, "VectorIndexerModel expected vector of length" +
      s" $numFeatures but found length ${features.size}")
    features match {
      case dv: DenseVector =>
        var hasInvalid = false
        val tmpv = dv.copy
        localVectorMap.foreach { case (featureIndex: Int, categoryMap: Map[Double, Int]) =>
          try {
            tmpv.values(featureIndex) = categoryMap(tmpv(featureIndex))
          } catch {
            case _: NoSuchElementException =>
              localHandleInvalid match {
                case HandleInvalid.Error =>
                  throw new IllegalArgumentException(s"VectorIndexer encountered invalid value " +
                    s"${tmpv(featureIndex)} on feature index $featureIndex. To handle " +
                    s"or skip invalid value, try setting VectorIndexer.handleInvalid.")
                case HandleInvalid.Keep =>
                  tmpv.values(featureIndex) = categoryMap.size
                case HandleInvalid.Skip =>
                  hasInvalid = true
              }
          }
        }
        if (hasInvalid) null else tmpv
      case sv: SparseVector =>
        // We use the fact that categorical value 0 is always mapped to index 0.
        var hasInvalid = false
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
            try {
              tmpv.values(k) = localVectorMap(featureIndex)(tmpv.values(k))
            } catch {
              case _: NoSuchElementException =>
                localHandleInvalid match {
                  case HandleInvalid.Error =>
                    throw new IllegalArgumentException(s"VectorIndexer encountered invalid value " +
                      s"${tmpv.values(k)} on feature index $featureIndex. To handle " +
                      s"or skip invalid value, try setting VectorIndexer.handleInvalid.")
                  case HandleInvalid.Keep =>
                    tmpv.values(k) = localVectorMap(featureIndex).size
                  case HandleInvalid.Skip =>
                    hasInvalid = true
                }
            }
            catFeatureIdx += 1
            k += 1
          }
        }
        if (hasInvalid) null else tmpv
    }
  }

  override def inputSchema: StructType = StructType("input" -> TensorType.Double(localNumFeatures)).get

  override def outputSchema: StructType = StructType("output" -> TensorType.Double(localNumFeatures)).get

}
