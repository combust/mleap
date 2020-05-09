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

  case object Keep extends HandleInvalid {
    override def asParamString: String = "keep"
  }

  def fromString(value: String): HandleInvalid = value match {
    case "error" => HandleInvalid.Error
    case "skip" => HandleInvalid.Skip
    case "keep" => HandleInvalid.Keep
    case _ => throw new IllegalArgumentException(s"Invalid handler: $value")
  }
}
