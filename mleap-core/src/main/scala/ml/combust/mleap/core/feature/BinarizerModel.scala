package ml.combust.mleap.core.feature

import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{BasicType, DataShape}
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable

/**
  * Created by mikhail on 10/16/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/feature/Binarizer.scala")
case class BinarizerModel(threshold: Double,
                          base: BasicType,
                          inputShape: DataShape) extends Serializable {
  assert(inputShape.isScalar || inputShape.isTensor, "Must provide a tensor or scalar shape")

  def apply(value: Double): Double = {
    if (value > threshold) 1.0 else 0.0
  }

  def apply(value: Vector): Vector = {
    val indices = mutable.ArrayBuilder.make[Int]
    val values = mutable.ArrayBuilder.make[Double]

    value.foreachActive { (index, value) =>
      if (value > threshold) {
        indices += index
        values += 1.0
      }
    }
    Vectors.sparse(value.size, indices.result(), values.result()).compressed
  }
}