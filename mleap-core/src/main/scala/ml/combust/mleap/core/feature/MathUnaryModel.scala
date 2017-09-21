package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType}

/**
  * Created by hollinwilkins on 12/27/16.
  */
sealed trait UnaryOperation {
  def name: String
}
object UnaryOperation {
  case object Log extends UnaryOperation {
    override def name: String = "log"
  }
  case object Exp extends UnaryOperation {
    override def name: String = "exp"
  }
  case object Sqrt extends UnaryOperation {
    override def name: String = "sqrt"
  }
  case object Sin extends UnaryOperation {
    override def name: String = "sin"
  }
  case object Cos extends UnaryOperation {
    override def name: String = "cos"
  }
  case object Tan extends UnaryOperation {
    override def name: String = "tan"
  }

  val all = Set(Log, Exp, Sqrt, Sin, Cos, Tan)
  val forName: Map[String, UnaryOperation] = all.map(o => (o.name, o)).toMap
}

case class MathUnaryModel(operation: UnaryOperation) extends Model {
  import UnaryOperation._

  def apply(a: Double): Double = operation match {
    case Log => Math.log(a)
    case Exp => Math.exp(a)
    case Sqrt => Math.sqrt(a)
    case Sin => Math.sin(a)
    case Cos => Math.cos(a)
    case Tan => Math.tan(a)
    case _ => throw new RuntimeException(s"unsupported unary operation: $operation")
  }

  override def inputSchema: StructType = StructType(
    "input" -> ScalarType.Double.nonNullable).get

  override def outputSchema: StructType = StructType(
    "output" -> ScalarType.Double.nonNullable).get
}
