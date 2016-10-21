package ml.combust.mleap.runtime.function

import ml.combust.mleap.runtime.types._

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by hollinwilkins on 10/19/16.
  */
object UserDefinedFunction {
  import ml.combust.mleap.runtime.reflection.MleapReflection.dataType

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
}

case class UserDefinedFunction(f: AnyRef,
                               returnType: DataType,
                               inputs: Seq[DataType])
