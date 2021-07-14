package ml.combust.mleap.core.reflection

import ml.combust.mleap.core.types._
import ml.combust.mleap.tensor.{ByteString, Tensor}

/**
  * Created by hollinwilkins on 10/21/16.
  */
object MleapReflectionLock

trait MleapReflection {
  val universe: scala.reflect.runtime.universe.type
  def mirror: universe.Mirror

  import universe._

  def typeSpec[T: TypeTag]: TypeSpec = {
    mirrorType[T] match {
      case t if t <:< mirrorType[Product] =>
        val TypeRef(_, _, sdts) = t
        val dts = sdts.map(dataTypeFor)
        SchemaSpec(dts)
      case t => DataTypeSpec(dataTypeFor(t))
    }
  }

  def dataType[T: TypeTag]: DataType = dataTypeFor(mirrorType[T])

  private def basicTypeFor(tpe: `Type`): BasicType = MleapReflectionLock.synchronized {
    tpe match {
      case t if t <:< mirrorType[Boolean] => BasicType.Boolean
      case t if t <:< mirrorType[Byte] => BasicType.Byte
      case t if t <:< mirrorType[Short] => BasicType.Short
      case t if t <:< mirrorType[Int] => BasicType.Int
      case t if t <:< mirrorType[Long] => BasicType.Long
      case t if t <:< mirrorType[Float] => BasicType.Float
      case t if t <:< mirrorType[Double] => BasicType.Double
      case t if t <:< mirrorType[String] => BasicType.String
      case t if t <:< mirrorType[ByteString] => BasicType.ByteString
      case _ => throw new IllegalArgumentException(s"invalid basic type: $tpe")
    }
  }

  private def dataTypeFor(tpe: `Type`): DataType = MleapReflectionLock.synchronized {
    tpe match {
      case t if t <:< mirrorType[Boolean] => ScalarType(BasicType.Boolean).nonNullable
      case t if t <:< mirrorType[Byte] => ScalarType(BasicType.Byte).nonNullable
      case t if t <:< mirrorType[Short] => ScalarType(BasicType.Short).nonNullable
      case t if t <:< mirrorType[Int] => ScalarType(BasicType.Int).nonNullable
      case t if t <:< mirrorType[Long] => ScalarType(BasicType.Long).nonNullable
      case t if t <:< mirrorType[Float] => ScalarType(BasicType.Float).nonNullable
      case t if t <:< mirrorType[Double] => ScalarType(BasicType.Double).nonNullable

      case t if t <:< mirrorType[String] => ScalarType(BasicType.String)
      case t if t <:< mirrorType[ByteString] => ScalarType(BasicType.ByteString)
      case t if t <:< mirrorType[java.lang.Boolean] => ScalarType(BasicType.Boolean)
      case t if t <:< mirrorType[java.lang.Byte] => ScalarType(BasicType.Byte)
      case t if t <:< mirrorType[java.lang.Short] => ScalarType(BasicType.Short)
      case t if t <:< mirrorType[java.lang.Integer] => ScalarType(BasicType.Int)
      case t if t <:< mirrorType[java.lang.Long] => ScalarType(BasicType.Long)
      case t if t <:< mirrorType[java.lang.Float] => ScalarType(BasicType.Float)
      case t if t <:< mirrorType[java.lang.Double] => ScalarType(BasicType.Double)

      case t if t <:< mirrorType[Seq[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        ListType(basicTypeFor(elementType))
      case t if t <:< mirrorType[Tensor[_]] =>
        val TypeRef(_, _, Seq(elementType)) = t
        TensorType(basicTypeFor(elementType))
      case t => throw new IllegalArgumentException(s"unknown type $t")
    }
  }

  private def mirrorType[T: TypeTag]: `Type` = MleapReflectionLock.synchronized {
    typeTag[T].in(mirror).tpe.dealias
  }

  def extractConstructorParameters[T: TypeTag] : Seq[(String, DataType)] = MleapReflectionLock.synchronized {
    val tpe = mirrorType[T]
    tpe match {
      case t if representsCaseClass(t) =>
        val formalTypeArgs = t.typeSymbol.asClass.typeParams
        val TypeRef(_, _, actualTypeArgs) = t
        constructParams(t).map { p =>
          p.name.toString -> dataTypeFor(p.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs))
        }
      case t => throw new IllegalArgumentException(s"unknown type $t")
    }
  }

  private def representsCaseClass(tpe: Type): Boolean = {
    tpe <:< mirrorType[Product] && tpe.typeSymbol.asClass.isCaseClass
  }

  private def constructParams(tpe: Type): Seq[Symbol] = {
    constructorSymbol(tpe).paramLists.flatten
  }

  private def constructorSymbol(tpe: universe.Type) : MethodSymbol = {
    val constructorSymbol = tpe.member(termNames.CONSTRUCTOR)
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

  def newInstance[T: TypeTag](args: Seq[_]) : T = MleapReflectionLock.synchronized {
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
