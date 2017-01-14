package ml.combust.mleap.runtime.reflection

import ml.combust.mleap.core.Tensor
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.types._

/**
  * Created by hollinwilkins on 10/21/16.
  */
object MleapReflectionLock

trait MleapReflection {
  val universe: scala.reflect.runtime.universe.type
  def mirror: universe.Mirror

  import universe._

  def dataType[T: TypeTag](implicit context: MleapContext): DataType = dataTypeFor(mirrorType[T])
  private def dataTypeFor(tpe: `Type`)
                         (implicit context: MleapContext): DataType = MleapReflectionLock.synchronized {
    tpe match {
      case t if t <:< mirrorType[Boolean] => BooleanType(false)
      case t if t <:< mirrorType[String] => StringType(false)
      case t if t <:< mirrorType[Int] => IntegerType(false)
      case t if t <:< mirrorType[Long] => LongType(false)
      case t if t <:< mirrorType[Float] => FloatType(false)
      case t if t <:< mirrorType[Double] => DoubleType(false)
      case t if t <:< mirrorType[Seq[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        val baseType = dataTypeFor(elementType)
        ListType(baseType)
      case t if t <:< mirrorType[Tensor[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        val baseType = dataTypeFor(elementType)
        TensorType(baseType.asInstanceOf[BasicType])
      case t if t =:= mirrorType[Any] => AnyType(false)
      case t if context.hasCustomType(t.erasure.typeSymbol.asClass.fullName) =>
        context.customType(t.erasure.typeSymbol.asClass.fullName)
      case t if t <:< mirrorType[Option[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        val baseType = dataTypeFor(elementType)
        baseType.asNullable
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
