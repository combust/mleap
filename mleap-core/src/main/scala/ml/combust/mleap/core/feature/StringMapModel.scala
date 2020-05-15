package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType}

/** Class for string map model.
 *
 * Maps a string into a double.
 *
 * @param labels map of labels and values
 * @param handleInvalid how to handle missing labels: 'error' (throw an error),
 *                      or 'keep' (map to the default value)
 * @param defaultValue value to use if label is not found in the map
 */
case class StringMapModel(labels: Map[String, Double],
                          handleInvalid: HandleInvalid = HandleInvalid.default,
                          defaultValue: Double = StringMapModel.defaultValue) extends Model {

  private val keepInvalid = handleInvalid == HandleInvalid.Keep

  def apply(label: String): Double = {
    if (keepInvalid) {
      labels.getOrElse(label, defaultValue)
    } else {
      if (labels.contains(label)) {
        labels(label)
      } else {
        throw new NoSuchElementException(s"Missing label: $label. To handle unseen labels, " +
          s"set handleInvalid to ${HandleInvalid.Keep.asParamString} and optionally set a custom defaultValue")
      }
    }
  }

  override def inputSchema: StructType = StructType("input" -> ScalarType.String).get

  override def outputSchema: StructType = StructType("output" -> ScalarType.Double.nonNullable).get
}

object StringMapModel {
  val defaultValue = 0.0
}
