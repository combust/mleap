package ml.combust.mleap.core.recommendation

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{ScalarType, StructType}
import org.apache.spark.ml.linalg.Vector

@SparkCode(uri = "https://github.com/apache/spark/blob/v2.3.0/mllib/src/main/scala/org/apache/spark/ml/recommendation/ALS.scala")
case class ALSModel(rank: Integer, userFactors: Map[Int, Vector], itemFactors: Map[Int, Vector]) extends Model {

  def apply(userId: Integer, itemId: Integer): Float = {
    val ufs = userFactors.get(userId)
    val ifs = itemFactors.get(itemId)

    if (ufs.nonEmpty && ifs.nonEmpty) {
      val featuresA = ufs.get
      val featuresB = ifs.get

      var dotProduct = 0.0
      var i = 0
      while (i < rank) {
        dotProduct += featuresA(i) * featuresB(i)
        i += 1
      }
      dotProduct.toFloat
    } else {
      Float.NaN
    }
  }

  override def inputSchema: StructType = StructType("user" -> ScalarType.Int.nonNullable,
                                                    "item" -> ScalarType.Int.nonNullable).get

  override def outputSchema: StructType = StructType("prediction" -> ScalarType.Float.nonNullable).get
}
