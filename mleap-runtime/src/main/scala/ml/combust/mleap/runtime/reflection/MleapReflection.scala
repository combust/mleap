package ml.combust.mleap.runtime.reflection

import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.types._
import ml.combust.mleap.tensor.Tensor

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

  private def mirrorType[T: TypeTag]: `Type` = MleapReflectionLock.synchronized {
    typeTag[T].in(mirror).tpe.normalize
  }

  def extractConstructorParameters[T: TypeTag] : Seq[(String, DataType)] = MleapReflectionLock.synchronized {
    val tpe = mirrorType[T]
    tpe match {
      case t if representsCaseClass(t) =>
        val formalTypeArgs = t.typeSymbol.asClass.typeParams
        val TypeRef(_, _, actualTypeArgs) = t
        constructParams(tpe).map { p =>
          p.name.toString -> dataTypeFor(p.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs))
        }
      case t => throw new IllegalArgumentException(s"unknown type $t")
    }
  }

  private def representsCaseClass(tpe: Type): Boolean = {
    tpe <:< mirrorType[Product] && tpe.typeSymbol.asClass.isCaseClass
  }

  private def constructParams(tpe: Type): Seq[Symbol] = {
    constructorSymbol(tpe).paramss.flatten
  }

  private def constructorSymbol(tpe: universe.Type) : MethodSymbol = {
    val constructorSymbol = tpe.member(nme.CONSTRUCTOR)
    if (constructorSymbol.isMethod) {
      constructorSymbol.asMethod
    } else {
      val primaryConstructorSymbol: Option[Symbol] = constructorSymbol.asTerm.alternatives.find(
        s => s.isMethod && s.asMethod.isPrimaryConstructor)
      if (primaryConstructorSymbol.isEmpty) {
        throw new IllegalArgumentException(s"type $tpe did not have a primary constructor")
      } else {
        primaryConstructorSymbol.get.asMethod
      }
    }
  }

  def newInstance[T:TypeTag](args: Seq[_]) : T = MleapReflectionLock.synchronized {
    val tpe = mirrorType[T]
    tpe match {
      case t if representsCaseClass(t) =>
        val constructor = constructorSymbol(t)
        val classMirror = mirror.reflectClass(t.typeSymbol.asClass)
        classMirror.reflectConstructor(constructor).apply(args: _*).asInstanceOf[T]
      case t => throw new IllegalArgumentException(s"unknown type $t")
    }
  }
}

object MleapReflection extends MleapReflection {
  override val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
  override def mirror: universe.Mirror = MleapReflectionLock.synchronized {
    universe.runtimeMirror(Thread.currentThread().getContextClassLoader)
  }
}
