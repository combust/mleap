package ml.combust.mleap.core.types

import ml.combust.mleap.tensor.Tensor

import scala.util.{Failure, Success, Try}

/**
  * Created by hollinwilkins on 9/1/17.
  */
object Casting {
  val basicCast: Map[(BasicType, BasicType), AnyRef] = Map(
    (BasicType.Boolean, BasicType.Byte) -> { (v: Boolean) => (if (v) 1 else 0).toByte },
    (BasicType.Boolean, BasicType.Short) -> { (v: Boolean) => (if (v) 1 else 0).toShort },
    (BasicType.Boolean, BasicType.Int) -> { (v: Boolean) => if (v) 1 else 0 },
    (BasicType.Boolean, BasicType.Long) -> { (v: Boolean) => (if (v) 1 else 0).toLong },
    (BasicType.Boolean, BasicType.Float) -> { (v: Boolean) => (if (v) 1 else 0).toFloat },
    (BasicType.Boolean, BasicType.Double) -> { (v: Boolean) => (if (v) 1 else 0).toDouble },
    (BasicType.Boolean, BasicType.String) -> { (v: Boolean) => (if (v) 1 else 0).toString },

    (BasicType.Byte, BasicType.Boolean) -> { (v: Byte) => if (v != 0) true else false },
    (BasicType.Byte, BasicType.Short) -> { (v: Byte) => v.toShort },
    (BasicType.Byte, BasicType.Int) -> { (v: Byte) => v.toInt },
    (BasicType.Byte, BasicType.Long) -> { (v: Byte) => v.toLong },
    (BasicType.Byte, BasicType.Float) -> { (v: Byte) => v.toFloat },
    (BasicType.Byte, BasicType.Double) -> { (v: Byte) => v.toDouble },
    (BasicType.Byte, BasicType.String) -> { (v: Byte) => v.toString },

    (BasicType.Short, BasicType.Boolean) -> { (v: Short) => if (v != 0) true else false },
    (BasicType.Short, BasicType.Byte) -> { (v: Short) => v.toByte },
    (BasicType.Short, BasicType.Int) -> { (v: Short) => v.toByte },
    (BasicType.Short, BasicType.Long) -> { (v: Short) => v.toByte },
    (BasicType.Short, BasicType.Float) -> { (v: Short) => v.toByte },
    (BasicType.Short, BasicType.Double) -> { (v: Short) => v.toByte },
    (BasicType.Short, BasicType.String) -> { (v: Short) => v.toString },

    (BasicType.Int, BasicType.Boolean) -> { (v: Int) => if (v != 0) true else false },
    (BasicType.Int, BasicType.Byte) -> { (v: Int) => v.toByte },
    (BasicType.Int, BasicType.Short) -> { (v: Int) => v.toShort },
    (BasicType.Int, BasicType.Long) -> { (v: Int) => v.toLong },
    (BasicType.Int, BasicType.Float) -> { (v: Int) => v.toFloat },
    (BasicType.Int, BasicType.Double) -> { (v: Int) => v.toDouble },
    (BasicType.Int, BasicType.String) -> { (v: Int) => v.toString },

    (BasicType.Long, BasicType.Boolean) -> { (v: Long) => if (v != 0) true else false },
    (BasicType.Long, BasicType.Byte) -> { (v: Long) => v.toByte },
    (BasicType.Long, BasicType.Short) -> { (v: Long) => v.toShort },
    (BasicType.Long, BasicType.Int) -> { (v: Long) => v.toInt },
    (BasicType.Long, BasicType.Float) -> { (v: Long) => v.toFloat },
    (BasicType.Long, BasicType.Double) -> { (v: Long) => v.toDouble },
    (BasicType.Long, BasicType.String) -> { (v: Long) => v.toString },

    (BasicType.Float, BasicType.Boolean) -> { (v: Float) => if (v != 0.0) true else false },
    (BasicType.Float, BasicType.Byte) -> { (v: Float) => v.toByte },
    (BasicType.Float, BasicType.Short) -> { (v: Float) => v.toShort },
    (BasicType.Float, BasicType.Int) -> { (v: Float) => v.toInt },
    (BasicType.Float, BasicType.Long) -> { (v: Float) => v.toLong },
    (BasicType.Float, BasicType.Double) -> { (v: Float) => v.toDouble },
    (BasicType.Float, BasicType.String) -> { (v: Float) => v.toString },

    (BasicType.Double, BasicType.Boolean) -> { (v: Double) => if (v != 0.0) true else false },
    (BasicType.Double, BasicType.Byte) -> { (v: Double) => v.toByte },
    (BasicType.Double, BasicType.Short) -> { (v: Double) => v.toShort },
    (BasicType.Double, BasicType.Int) -> { (v: Double) => v.toInt },
    (BasicType.Double, BasicType.Long) -> { (v: Double) => v.toLong },
    (BasicType.Double, BasicType.Float) -> { (v: Double) => v.toFloat },
    (BasicType.Double, BasicType.String) -> { (v: Double) => v.toString },

    (BasicType.String, BasicType.Boolean) -> { (v: String) => v match {
      case "true" | "yes" => true
      case "false" | "no" => false
      case digits if v.forall(_.isDigit) => digits.toDouble != 0
      case _ => true
    }
    },
    (BasicType.String, BasicType.Byte) -> { (v: String) => v.toByte },
    (BasicType.String, BasicType.Short) -> { (v: String) => v.toShort },
    (BasicType.String, BasicType.Int) -> { (v: String) => v.toInt },
    (BasicType.String, BasicType.Long) -> { (v: String) => v.toLong },
    (BasicType.String, BasicType.Float) -> { (v: String) => v.toFloat },
    (BasicType.String, BasicType.Double) -> { (v: String) => v.toDouble }
  )

  def cast(from: DataType, to: DataType): Try[(Any) => Any] = {
    if(from.shape.getClass != to.shape.getClass) {
      return Failure(new IllegalArgumentException("Shapes of casting from -> to do not match"))
    }

    val c = (basicCast.get((from.base, to.base)) match {
      case Some(c) => Success(c)
      case None => Failure(new IllegalArgumentException(s"Cannot cast base type ${from.base} -> ${to.base}"))
    }).get.asInstanceOf[(Any) => Any]

    (from, to) match {
      case (_: ScalarType, _: ScalarType) => Success(c)
      case (_: ListType, _: ListType) =>
        Success((l: Any) => l.asInstanceOf[Seq[Any]].map(c))
      case (f: TensorType, t: TensorType) if f.shape == t.shape =>
        Success((l: Any) => l.asInstanceOf[Tensor[_]].mapValues(c))
      case _ => Failure(new IllegalArgumentException(s"Cannot cast $from to $to"))
    }
  }
}
