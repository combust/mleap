package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{ScalarType, StructType}

/** Class for a bucketizer model.
  *
  * Bucketizer will place incoming feature into a bucket.
  *
  * @param splits splits used to determine bucket
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/feature/Bucketizer.scala")
case class BucketizerModel(splits: Array[Double], handleInvalid: String = "error") extends Model {
  def apply(feature: Double): Double = {
    if (handleInvalid == "skip") {
      -1
    } else {
      binarySearchForBuckets(splits, feature, keepInvalid = handleInvalid == "keep")
    }
  }

  def binarySearchForBuckets(splits: Array[Double], feature: Double, keepInvalid: Boolean): Double = {
    if (feature.isNaN) {
      if (keepInvalid) {
        splits.length - 1
      } else {
        throw new RuntimeException("Bucketizer encountered NaN value. To handle or skip NaNs," +
          " try setting Bucketizer.handleInvalid.")
      }
    } else if (feature == splits.last) {
      splits.length - 2
    } else {
      val idx = java.util.Arrays.binarySearch(splits, feature)
      if (idx >= 0) {
        idx
      } else {
        val insertPos = -idx - 1
        if (insertPos == 0 || insertPos == splits.length) {
          throw new RuntimeException(s"Feature value $feature out of Bucketizer bounds" +
            s" [${splits.head}, ${splits.last}]. Check your features, or loosen " +
            s"the lower/upper bound constraints.")
        } else {
          insertPos - 1
        }
      }
    }
  }

  override def inputSchema: StructType = StructType("input" -> ScalarType.Double.nonNullable).get

  override def outputSchema: StructType = StructType("output" -> ScalarType.Double.nonNullable).get
}
