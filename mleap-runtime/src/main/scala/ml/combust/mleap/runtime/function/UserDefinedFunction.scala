package ml.combust.mleap.runtime.function

import ml.combust.mleap.runtime.types._
import org.apache.spark.ml.linalg.Vector

import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 10/19/16.
  */
object UserDefinedFunction {
  val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
  def mirror: universe.Mirror = synchronized {
    universe.runtimeMirror(Thread.currentThread().getContextClassLoader)
  }
  import universe._

  implicit def apply[RT: TypeTag](f: () => RT): UserDefinedFunction =
    UserDefinedFunction(f, dataType[RT], Seq())
  implicit def apply[RT: TypeTag, T1: TypeTag](f: (T1) => RT): UserDefinedFunction =
    UserDefinedFunction(f, dataType[RT], Seq(dataType[T1]))
  implicit def apply[RT: TypeTag, T1: TypeTag, T2: TypeTag](f: (T1, T2) => RT): UserDefinedFunction =
    UserDefinedFunction(f, dataType[RT], Seq(dataType[T1], dataType[T2]))
  implicit def apply[RT: TypeTag, T1: TypeTag, T2: TypeTag, T3: TypeTag](f: (T1, T2, T3) => RT): UserDefinedFunction =
    UserDefinedFunction(f, dataType[RT], Seq(dataType[T1], dataType[T2], dataType[T3]))
  implicit def apply[RT: TypeTag, T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag](f: (T1, T2, T3, T4) => RT): UserDefinedFunction =
    UserDefinedFunction(f, dataType[RT], Seq(dataType[T1], dataType[T2], dataType[T3], dataType[T4]))
  implicit def apply[RT: TypeTag, T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag, T5: TypeTag](f: (T1, T2, T3, T4, T5) => RT): UserDefinedFunction =
    UserDefinedFunction(f, dataType[RT], Seq(dataType[T1], dataType[T2], dataType[T3], dataType[T4], dataType[T5]))

  private def dataType[T: TypeTag]: DataType = dataTypeFor(mirrorType[T])
  private def dataTypeFor(tpe: `Type`): DataType = synchronized {
    tpe match {
      case t if t <:< mirrorType[Boolean] => BooleanType
      case t if t <:< mirrorType[String] => StringType
      case t if t <:< mirrorType[Long] => LongType
      case t if t <:< mirrorType[Double] => DoubleType
      case t if t <:< mirrorType[Array[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        val baseType = dataTypeFor(elementType)
        ListType(baseType)
      case t if t <:< mirrorType[Vector] => TensorType.doubleVector()
      case t if t <:< mirrorType[Any] => AnyType
      case t => throw new IllegalArgumentException(s"unknown type $t")
    }
  }

  private def mirrorType[T: TypeTag]: `Type` = typeTag[T].in(mirror).tpe.normalize
}

case class UserDefinedFunction(f: AnyRef,
                               returnType: DataType,
                               inputs: Seq[DataType])
