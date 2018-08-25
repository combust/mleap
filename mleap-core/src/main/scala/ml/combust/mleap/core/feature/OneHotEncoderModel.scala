package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types._
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
case class OneHotEncoderModel(categorySizes: Array[Int],
                              handleInvalid: HandleInvalid = HandleInvalid.Error,
                              dropLast: Boolean = true) extends Model {
  private val oneValue = Array(1.0)
  private val emptyIndices = Array.empty[Int]
  private val emptyValues = Array.empty[Double]
  private val keepInvalid: Boolean = handleInvalid == HandleInvalid.Keep
  private val configedCategorySizes: Array[Int] = {

    if (!dropLast && keepInvalid) {
      // When `handleInvalid` is "keep", an extra category is added as last category
      // for invalid data.
      categorySizes.map(_ + 1)
    } else if (dropLast && !keepInvalid) {
      // When `dropLast` is true, the last category is removed.
      categorySizes.map(_ - 1)
    } else {
      // When `dropLast` is true and `handleInvalid` is "keep", the extra category for invalid
      // data is removed. Thus, it is the same as the plain number of categories.
      categorySizes
    }
  }

  /** Turn an array of labeled features into an array of one hot vectors.
    *
    * @param labels array pf labels to convert to vectors
    * @return array pf one hot vector representations of labels
    */
  def apply(labels: Array[Double]): Array[Vector] = {
    if (labels.length != categorySizes.length) {
      throw new IllegalArgumentException(s"invalid input size: ${labels.length}, must be ${categorySizes.length}")
    }
    labels.zipWithIndex.map {
      case (label: Double, colIdx: Int) ⇒ encoder(label, colIdx)
    }
  }

  private def encoder(label: Double, colIdx: Int): Vector = {
    val labelInt = label.toInt

    if(label != labelInt) {
      throw new IllegalArgumentException(s"invalid label: $label, must be integer")
    }

    val origCategorySize = categorySizes(colIdx)
    val idx = if (label >= 0 && label < origCategorySize) {
      label
    } else {
      if (keepInvalid) {
        origCategorySize
      } else {
        if (label < 0) {
          throw new IllegalArgumentException(s"Negative value: $label. Input can't be negative. To handle invalid values, set Param handleInvalid to ${HandleInvalid.Keep}")
        } else {
          throw new IllegalArgumentException(s"Unseen value: $label. To handle unseen values, set Param handleInvalid to ${HandleInvalid.Keep}")
        }
      }
    }
    val size = configedCategorySizes(colIdx)
    if (idx < size) {
      Vectors.sparse(size, Array(idx.toInt), oneValue)
    } else {
      Vectors.sparse(size, emptyIndices, emptyValues)
    }
  }

  override def inputSchema: StructType = {
    val f = categorySizes.zipWithIndex.map {
      case (_, i) => StructField(s"input$i", ScalarType.Double.setNullable(false))
    }
    StructType(f).get
  }

  override def outputSchema: StructType = {
    val f = categorySizes.zipWithIndex.map {
      case (size, i) => StructField(s"output$i", TensorType.Double(size))
    }
    StructType(f).get
  }
}
