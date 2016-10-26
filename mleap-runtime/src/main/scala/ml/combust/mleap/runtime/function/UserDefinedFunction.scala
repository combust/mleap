package ml.combust.mleap.runtime.function

import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.reflection.MleapReflection._
import ml.combust.mleap.runtime.types._

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

/** Companion object for creating user defined functions.
  */
object UserDefinedFunction {
  implicit def function0[RT: TypeTag](f: () => RT)
                                 (implicit context: MleapContext = MleapContext.defaultContext): UserDefinedFunction = {
    UserDefinedFunction(f, dataType[RT], Seq())
  }

  implicit def function1[RT: TypeTag, T1: TypeTag](f: (T1) => RT)
                                              (implicit context: MleapContext = MleapContext.defaultContext): UserDefinedFunction = {
    UserDefinedFunction(f, dataType[RT], Seq(dataType[T1]))
  }

  implicit def function2[RT: TypeTag, T1: TypeTag, T2: TypeTag](f: (T1, T2) => RT)
                                                           (implicit context: MleapContext = MleapContext.defaultContext): UserDefinedFunction = {
    UserDefinedFunction(f, dataType[RT], Seq(dataType[T1], dataType[T2]))
  }

  implicit def function3[RT: TypeTag, T1: TypeTag, T2: TypeTag, T3: TypeTag](f: (T1, T2, T3) => RT)
                                                                        (implicit context: MleapContext = MleapContext.defaultContext): UserDefinedFunction = {
    UserDefinedFunction(f, dataType[RT], Seq(dataType[T1], dataType[T2], dataType[T3]))
  }

  implicit def function4[RT: TypeTag, T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag](f: (T1, T2, T3, T4) => RT)
                                                                                     (implicit context: MleapContext = MleapContext.defaultContext): UserDefinedFunction = {
    UserDefinedFunction(f, dataType[RT], Seq(dataType[T1], dataType[T2], dataType[T3], dataType[T4]))
  }

  implicit def function5[RT: TypeTag, T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag, T5: TypeTag](f: (T1, T2, T3, T4, T5) => RT)
                                                                                                  (implicit context: MleapContext = MleapContext.defaultContext): UserDefinedFunction = {
    UserDefinedFunction(f, dataType[RT], Seq(dataType[T1], dataType[T2], dataType[T3], dataType[T4], dataType[T5]))
  }
}

case class UserDefinedFunction(f: AnyRef,
                               returnType: DataType,
                               inputs: Seq[DataType])
