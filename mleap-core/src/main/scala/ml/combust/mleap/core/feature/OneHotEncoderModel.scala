package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}
import org.apache.spark.ml.linalg.{Vector, Vectors}

/** Class for a one hot encoder model.
  *
  * One hot encoders are used to vectorize nominal features
  * in preparation for models such as linear regression or
  * logistic regression where binary and not multinomial features
  * are supported in the feature vector.
  *
  * @param size size of the output one hot vectors
  */
case class OneHotEncoderModel(size: Int,
                              dropLast: Boolean = true) extends Model {
  private val oneValue = Array(1.0)
  private val emptyIndices = Array[Int]()
  private val emptyValues = Array[Double]()

  /** Turn a labeled feature into a one hot vector.
    *
    * @param label label to convert to a vector
    * @return one hot vector representation of label
    */
  def apply(label: Double): Vector = {
    val labelInt = label.toInt

    if(label != labelInt) {
      throw new IllegalArgumentException(s"invalid label: $label, must be integer")
    }

    if(label < size) {
      Vectors.sparse(size, Array(labelInt), oneValue)
    } else {
      Vectors.sparse(size, emptyIndices, emptyValues)
    }
  }

  override def inputSchema: StructType = StructType("input" -> ScalarType.Double).get

  override def outputSchema: StructType = StructType("output" -> TensorType.Double(size)).get
}
