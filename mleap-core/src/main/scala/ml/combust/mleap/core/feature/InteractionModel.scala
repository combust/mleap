package ml.combust.mleap.core.feature

import ml.combust.mleap.core.annotation.SparkCode
import org.apache.spark.ml.linalg.mleap.Vector._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

/**
  * Created by mikhail on 12/18/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/feature/Interaction.scala")
case class InteractionModel extends Serializable{
  def apply(vector: Vector): Vector = {

  }
}
