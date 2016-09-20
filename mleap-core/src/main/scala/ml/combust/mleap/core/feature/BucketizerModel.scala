package ml.combust.mleap.core.feature

import java.{util => ju}

/**
  * Created by mikhail on 9/18/16.
  */
class BucketizerModel(
                     splits: Array[Double]
                     ) extends Serializable{

  def apply(feature: Double): Double = {

    val bucket = binarySearchForBuckets(splits, feature)

    bucket

  }

  def binarySearchForBuckets(splits: Array[Double], feature: Double): Double = {
    if (feature == splits.last) {
      splits.length - 2
    } else {
      val idx = ju.Arrays.binarySearch(splits, feature)
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
}
