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
case class BucketizerModel(splits: Array[Double]) extends Model {
  def apply(feature: Double): Double = {
    binarySearchForBuckets(splits, feature)
  }

  def binarySearchForBuckets(splits: Array[Double], feature: Double): Double = {
    if (feature == splits.last) {
      splits.length - 2
    } else {
      val idx = java.util.Arrays.binarySearch(splits, feature)
      if (idx >= 0) {
        idx
      } else {
        val insertPos = -idx - 1
        if (insertPos == 0 || insertPos == splits.length) {
          throw new Exception(s"Feature value $feature out of Bucketizer bounds" +
            s" [${splits.head}, ${splits.last}].  Check your features, or loosen " +
            s"the lower/upper bound constraints.")
        } else {
          insertPos - 1
        }
      }
    }
  }

  override def inputSchema: StructType = StructType("input" -> ScalarType.Double).get

  override def outputSchema: StructType = StructType("output" -> ScalarType.Double).get
}
