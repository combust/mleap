package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model

/**
  * Created by hollinwilkins on 12/27/16.
  */
sealed trait BinaryOperation extends Serializable {
  def name: String
}
object BinaryOperation {
  case object Add extends BinaryOperation {
    override def name: String = "add"
  }
  case object Subtract extends BinaryOperation {
    override def name: String = "sub"
  }
  case object Multiply extends BinaryOperation {
    override def name: String = "mul"
  }
  case object Divide extends BinaryOperation {
    override def name: String = "div"
  }
  case object Remainder extends BinaryOperation {
    override def name: String = "rem"
  }
  case object LogN extends BinaryOperation {
    override def name: String = "log_n"
  }
  case object Pow extends BinaryOperation {
    override def name: String = "pow"
  }

  val all = Set(Add, Subtract, Multiply, Divide, Remainder, LogN, Pow)
  val forName: Map[String, BinaryOperation] = all.map(o => (o.name, o)).toMap
}

case class MathBinaryModel(operation: BinaryOperation,
                           da: Option[Double] = None,
                           db: Option[Double] = None) extends Model {
  import BinaryOperation._

  def apply(ma: Option[Double], mb: Option[Double]): Double = {
    val a = ma.getOrElse(da.get)
    val b = mb.getOrElse(db.get)

    operation match {
      case Add => a + b
      case Subtract => a - b
      case Multiply => a * b
      case Divide => a / b
      case Remainder => a % b
      case LogN => Math.log(a) / Math.log(b)
      case Pow => Math.pow(a, b)
      case _ => throw new RuntimeException(s"unsupported binary operation $operation")
    }
  }
}
