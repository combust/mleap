package ml.combust.mleap.core.feature

import ml.combust.mleap.core.annotation.SparkCode
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable

/** Companion object for defaults.
  */
object VectorAssemblerModel {
  val default: VectorAssemblerModel = VectorAssemblerModel()
}

/** Class for a vector assembler model.
  *
  * Vector assemblers take an input set of doubles and vectors
  * and create a new vector out of them. This is primarily used
  * to get all desired features into one vector before training
  * a model.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/feature/VectorAssembler.scala")
case class VectorAssemblerModel() extends Serializable {
  /** Assemble a feature vector from a set of input features.
    *
    * @param vv all input feature values
    * @return assembled vector
    */
  def apply(vv: Any *): Vector = {
    val indices = mutable.ArrayBuilder.make[Int]
    val values = mutable.ArrayBuilder.make[Double]
    var cur = 0
    vv.foreach {
      case v: Double =>
        if (v != 0.0) {
          indices += cur
          values += v
        }
        cur += 1
      case vec: Vector =>
        vec.foreachActive { case (i, v) =>
          if (v != 0.0) {
            indices += cur + i
            values += v
          }
        }
        cur += vec.size
    }
    Vectors.sparse(cur, indices.result(), values.result()).compressed
  }
}
