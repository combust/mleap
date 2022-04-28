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

  def typeSpec[T: TypeTag]: TypeSpec = cleanUpReflectionObjects {
    mirrorType[T] match {
      case t if isSubtype(t, mirrorType[Product]) =>
        val TypeRef(_, _, sdts) = t
        val dts = sdts.map(dataTypeFor)
        SchemaSpec(dts)
      case t => DataTypeSpec(dataTypeFor(t))
    }
  }

  def dataType[T: TypeTag]: DataType = dataTypeFor(mirrorType[T])

  private def basicTypeFor(tpe: `Type`): BasicType = cleanUpReflectionObjects {
    tpe match {
      case t if isSubtype(t, mirrorType[Boolean]) => BasicType.Boolean
      case t if isSubtype(t, mirrorType[Byte]) => BasicType.Byte
      case t if isSubtype(t, mirrorType[Short]) => BasicType.Short
      case t if isSubtype(t, mirrorType[Int]) => BasicType.Int
      case t if isSubtype(t, mirrorType[Long]) => BasicType.Long
      case t if isSubtype(t, mirrorType[Float]) => BasicType.Float
      case t if isSubtype(t, mirrorType[Double]) => BasicType.Double
      case t if isSubtype(t, mirrorType[String])=> BasicType.String
      case t if isSubtype(t, mirrorType[ByteString]) => BasicType.ByteString
      case _ => throw new IllegalArgumentException(s"invalid basic type: $tpe")
    }
  }

  private def dataTypeFor(tpe: `Type`): DataType = cleanUpReflectionObjects {
    tpe match {
      case t if isSubtype(t, mirrorType[Boolean]) => ScalarType(BasicType.Boolean).nonNullable
      case t if isSubtype(t, mirrorType[Byte]) => ScalarType(BasicType.Byte).nonNullable
      case t if isSubtype(t, mirrorType[Short]) => ScalarType(BasicType.Short).nonNullable
      case t if isSubtype(t, mirrorType[Int]) => ScalarType(BasicType.Int).nonNullable
      case t if isSubtype(t, mirrorType[Long]) => ScalarType(BasicType.Long).nonNullable
      case t if isSubtype(t, mirrorType[Float]) => ScalarType(BasicType.Float).nonNullable
      case t if isSubtype(t, mirrorType[Double]) => ScalarType(BasicType.Double).nonNullable

      case t if isSubtype(t, mirrorType[String]) => ScalarType(BasicType.String)
      case t if isSubtype(t, mirrorType[ByteString]) => ScalarType(BasicType.ByteString)
      case t if isSubtype(t, mirrorType[java.lang.Boolean]) => ScalarType(BasicType.Boolean)
      case t if isSubtype(t, mirrorType[java.lang.Byte]) => ScalarType(BasicType.Byte)
      case t if isSubtype(t, mirrorType[java.lang.Short]) => ScalarType(BasicType.Short)
      case t if isSubtype(t, mirrorType[java.lang.Integer]) => ScalarType(BasicType.Int)
      case t if isSubtype(t, mirrorType[java.lang.Long]) => ScalarType(BasicType.Long)
      case t if isSubtype(t, mirrorType[java.lang.Float]) => ScalarType(BasicType.Float)
      case t if isSubtype(t, mirrorType[java.lang.Double]) => ScalarType(BasicType.Double)

      case t if isSubtype(t, mirrorType[Seq[_]]) =>
        val TypeRef(_, _, Seq(elementType)) = t
        ListType(basicTypeFor(elementType))
      case m if isSubtype(m, mirrorType[Map[_,_]]) =>
        val TypeRef(_, _, Seq(keyType, valueType)) = m
        MapType(basicTypeFor(keyType), basicTypeFor(valueType))
      case t if isSubtype(t, mirrorType[Tensor[_]]) =>
        val TypeRef(_, _, Seq(elementType)) = t
        TensorType(basicTypeFor(elementType))
      case t => throw new IllegalArgumentException(s"unknown type $t")
    }
  }

  private[reflection] def mirrorType[T: TypeTag]: `Type` = MleapReflectionLock.synchronized {
    typeTag[T].in(mirror).tpe.dealias
  }

  def extractConstructorParameters[T: TypeTag] : Seq[(String, DataType)] = cleanUpReflectionObjects {
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
    isSubtype(tpe, mirrorType[Product]) && tpe.typeSymbol.asClass.isCaseClass
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

  def newInstance[T: TypeTag](args: Seq[_]) : T = cleanUpReflectionObjects {
    val tpe = mirrorType[T]
    tpe match {
      case t if representsCaseClass(t) =>
        val constructor = constructorSymbol(t)
        val classMirror = mirror.reflectClass(t.typeSymbol.asClass)
        classMirror.reflectConstructor(constructor).apply(args: _*).asInstanceOf[T]
      case t => throw new IllegalArgumentException(s"unknown type $t")
    }
  }

  /**
    * Any code calling `scala.reflect.api.Types.TypeApi.<:<` should be wrapped by this method to
    * clean up the Scala reflection garbage automatically. Otherwise, it will leak some objects to
    * `scala.reflect.runtime.JavaUniverse.undoLog`.
    *
    * @see https://github.com/scala/bug/issues/8302
    */
  def cleanUpReflectionObjects[T](func: => T): T = MleapReflectionLock.synchronized {
    universe.asInstanceOf[scala.reflect.runtime.JavaUniverse].undoLog.undo(func)
  }

  /**
    * Synchronize to prevent concurrent usage of `<:<` operator.
    * This operator is not thread safe in any current version of scala; i.e.
    * (2.11.12, 2.12.10, 2.13.0-M5).
    *
    * See https://github.com/scala/bug/issues/10766
    */
  private[reflection] def isSubtype(tpe1: `Type`, tpe2: `Type`): Boolean = {
    MleapReflectionLock.synchronized {
      tpe1 <:< tpe2
    }
  }
}

object MleapReflection extends MleapReflection {
  override val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
  private var classLoader: Option[ClassLoader] = None

  def setClassLoader(cl: ClassLoader) = MleapReflectionLock.synchronized {
    classLoader = Some(cl)
  }

  def getClassLoader(): ClassLoader = MleapReflectionLock.synchronized {
    classLoader.getOrElse(Thread.currentThread().getContextClassLoader)
  }

  override def mirror: universe.Mirror = MleapReflectionLock.synchronized {
    universe.runtimeMirror(getClassLoader())
  }
}
