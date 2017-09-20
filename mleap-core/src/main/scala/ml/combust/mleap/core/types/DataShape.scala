package ml.combust.mleap.core.types

/**
  * Created by hollinwilkins on 6/30/17.
  */
sealed trait DataShape {
  def isScalar: Boolean = false
  def isList: Boolean = false
  def isTensor: Boolean = false

  val isNullable: Boolean
}

case class ScalarShape(override val isNullable: Boolean = true) extends DataShape {
  override def isScalar: Boolean = true
}
case class ListShape(override val isNullable: Boolean = true) extends DataShape {
  override def isList: Boolean = true
}

object TensorShape {
  def apply(dim0: Int, dims: Int *): TensorShape = TensorShape(Some(dim0 +: dims))
}

case class TensorShape(dimensions: Option[Seq[Int]] = None,
                       override val isNullable: Boolean = true) extends DataShape {
  override def isTensor: Boolean = true
}
