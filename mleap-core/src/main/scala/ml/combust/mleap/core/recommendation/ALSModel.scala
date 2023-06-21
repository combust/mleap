package ml.combust.mleap.core.recommendation

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{ScalarType, StructType}
import dev.ludovic.netlib.blas.BLAS.{getInstance => blas}

@SparkCode(uri = "https://github.com/apache/spark/blob/v2.2.0/mllib/src/main/scala/org/apache/spark/ml/recommendation/ALS.scala")
case class ALSModel(rank: Integer, userFactors: Map[Int, Array[Float]], itemFactors: Map[Int, Array[Float]]) extends Model {

  def apply(userId: Integer, itemId: Integer): Float = {
    val featuresA = userFactors.get(userId)
    val featuresB = itemFactors.get(itemId)

    if (featuresA.nonEmpty && featuresB.nonEmpty) {
      blas.sdot(rank, featuresA.get, 1, featuresB.get, 1)
    } else {
      Float.NaN
    }
  }

  override def inputSchema: StructType = StructType("user" -> ScalarType.Int.nonNullable,
                                                    "item" -> ScalarType.Int.nonNullable).get

  override def outputSchema: StructType = StructType("prediction" -> ScalarType.Float.nonNullable).get
}
