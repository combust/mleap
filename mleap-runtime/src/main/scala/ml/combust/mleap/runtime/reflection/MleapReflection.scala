package ml.combust.mleap.runtime.reflection

import ml.combust.mleap.runtime.MleapContext
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

  def dataType[T: TypeTag](implicit context: MleapContext): DataType = dataTypeFor(mirrorType[T])
  private def dataTypeFor(tpe: `Type`)
                         (implicit context: MleapContext): DataType = MleapReflectionLock.synchronized {
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
      case t if context.hasCustomType(t.erasure.typeSymbol.asClass.fullName) =>
        context.customTypeForClass[Any](t.erasure.typeSymbol.asClass.fullName)
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
