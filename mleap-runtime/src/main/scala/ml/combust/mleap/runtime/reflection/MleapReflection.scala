package ml.combust.mleap.runtime.reflection

import ml.combust.mleap.runtime.types._
import org.apache.spark.ml.linalg.Vector

/**
  * Created by hollinwilkins on 10/21/16.
  */
object MleapReflectionLock

trait MleapReflection {
  val universe: scala.reflect.runtime.universe.type
  def mirror: universe.Mirror

  import universe._

  def scalaType(t: DataType): `Type` = t match {
    case BooleanType => mirrorType[Boolean]
    case StringType => mirrorType[String]
    case IntegerType => mirrorType[Int]
    case LongType => mirrorType[Long]
    case DoubleType => mirrorType[Double]
    case ListType(base) => base match { // TODO: support arbitrary depth arrays
      case BooleanType => mirrorType[Array[Boolean]]
      case StringType => mirrorType[Array[String]]
      case IntegerType => mirrorType[Array[Int]]
      case LongType => mirrorType[Array[Long]]
      case DoubleType => mirrorType[Array[Double]]
      case AnyType => mirrorType[Array[Any]]
      case tt: TensorType if tt.dimensions.length == 1 => mirrorType[Array[Vector]]
      case _: ListType => throw new IllegalArgumentException("deep arrays not supported yet")
      case _ => throw new IllegalArgumentException(s"unknown base type: $base")
    }
    case tt: TensorType if tt.dimensions.length == 1 => mirrorType[Vector]
    case AnyType => mirrorType[Any]
    case _ => throw new IllegalArgumentException(s"unknown type: $t")
  }

  def dataType[T: TypeTag]: DataType = dataTypeFor(mirrorType[T])
  private def dataTypeFor(tpe: `Type`): DataType = MleapReflectionLock.synchronized {
    tpe match {
      case t if t <:< mirrorType[Boolean] => BooleanType
      case t if t <:< mirrorType[String] => StringType
      case t if t <:< mirrorType[Int] => IntegerType
      case t if t <:< mirrorType[Long] => LongType
      case t if t <:< mirrorType[Double] => DoubleType
      case t if t <:< mirrorType[Array[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        val baseType = dataTypeFor(elementType)
        ListType(baseType)
      case t if t <:< mirrorType[Vector] => TensorType.doubleVector()
      case t if t =:= mirrorType[Any] => AnyType
      case t => throw new IllegalArgumentException(s"unknown type $t")
    }
  }

  private def mirrorType[T: TypeTag]: `Type` = typeTag[T].in(mirror).tpe.normalize
}

object MleapReflection extends MleapReflection {
  override val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
  override def mirror: universe.Mirror = MleapReflectionLock.synchronized {
    universe.runtimeMirror(Thread.currentThread().getContextClassLoader)
  }
}
