package ml.combust.mleap.core.feature

sealed trait HandleInvalid {
  def asParamString: String
}

object HandleInvalid {
  val default = Error

  case object Error extends HandleInvalid {
    override def asParamString: String = "error"
  }

  case object Skip extends HandleInvalid {
    override def asParamString: String = "skip"
  }

  def fromString(value: String): HandleInvalid = value match {
    case "error" => HandleInvalid.Error
    case "skip" => HandleInvalid.Skip
    case _ => throw new IllegalArgumentException(s"Invalid handler: $value")
  }
}

/** Class for string indexer model.
  *
  * String indexer converts a string into an integer representation.
  *
  * @param labels list of labels that can be indexed
  * @param handleInvalid how to handle invalid values, doesn't do anything in MLeap Runtime
  */
case class StringIndexerModel(labels: Seq[String],
                              handleInvalid: HandleInvalid = HandleInvalid.Error) extends Serializable {
  private val stringToIndex: Map[String, Int] = labels.zipWithIndex.toMap

  /** Convert a string into its integer representation.
    *
    * @param value label to index
    * @return index of label
    */
  def apply(value: Any): Double = value match {
    case opt: Option[_] => stringToIndex(opt.get.toString)
    case _ => stringToIndex(value.toString)
  }

  /** Create a [[ml.combust.mleap.core.feature.ReverseStringIndexerModel]] from this model.
    *
    * @return reverse string indexer of this string indexer
    */
  def toReverse: ReverseStringIndexerModel = ReverseStringIndexerModel(labels)
}
