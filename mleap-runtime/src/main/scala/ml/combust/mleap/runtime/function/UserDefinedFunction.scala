package ml.combust.mleap.runtime.function

import ml.combust.mleap.core.reflection.MleapReflection._
import ml.combust.mleap.core.types.{DataType, StructType, TypeSpec}

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

/** Companion object for creating user defined functions. */
object UserDefinedFunction {
  def apply(f: AnyRef,
            output: StructType,
            input: StructType): UserDefinedFunction = {
    UserDefinedFunction(f, output: TypeSpec, input.fields.map(_.dataType: TypeSpec))
  }

  def apply(f: AnyRef,
            output: DataType,
            inputs: Seq[TypeSpec]): UserDefinedFunction = {
    UserDefinedFunction(f, output: TypeSpec, inputs)
  }

  def apply(f: AnyRef,
            output: TypeSpec,
            input0: DataType,
            inputs: DataType *): UserDefinedFunction = {
    UserDefinedFunction(f, output, (input0 +: inputs).map(d => d: TypeSpec))
  }

  def apply(f: AnyRef,
            output: DataType,
            input0: DataType,
            inputs: DataType *): UserDefinedFunction = {
    UserDefinedFunction(f, output, (input0 +: inputs).map(d => d: TypeSpec))
  }

  implicit def function0[RT: TypeTag](f: () => RT): UserDefinedFunction = {
    UserDefinedFunction(f, typeSpec[RT], Seq())
  }

  implicit def function1[RT: TypeTag, T1: TypeTag](f: (T1) => RT): UserDefinedFunction = {
    UserDefinedFunction(f, typeSpec[RT], dataType[T1])
  }

  implicit def function2[RT: TypeTag, T1: TypeTag, T2: TypeTag](f: (T1, T2) => RT): UserDefinedFunction = {
    UserDefinedFunction(f, typeSpec[RT], dataType[T1], dataType[T2])
  }

  implicit def function3[RT: TypeTag, T1: TypeTag, T2: TypeTag, T3: TypeTag](f: (T1, T2, T3) => RT): UserDefinedFunction = {
    UserDefinedFunction(f, typeSpec[RT], dataType[T1], dataType[T2], dataType[T3])
  }

  implicit def function4[RT: TypeTag, T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag](f: (T1, T2, T3, T4) => RT): UserDefinedFunction = {
    UserDefinedFunction(f, typeSpec[RT], dataType[T1], dataType[T2], dataType[T3], dataType[T4])
  }

  implicit def function5[RT: TypeTag, T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag, T5: TypeTag](f: (T1, T2, T3, T4, T5) => RT): UserDefinedFunction = {
    UserDefinedFunction(f, typeSpec[RT], dataType[T1], dataType[T2], dataType[T3], dataType[T4], dataType[T5])
  }
}

case class UserDefinedFunction(f: AnyRef,
                               output: TypeSpec,
                               inputs: Seq[TypeSpec]) {
  def outputTypes: Seq[DataType] = output.dataTypes

  def withInputs(inputs: Seq[TypeSpec]): UserDefinedFunction = copy(inputs = inputs)
  def withInputs(schema: StructType): UserDefinedFunction = withDataTypeInputs(schema.fields.map(_.dataType))
  def withDataTypeInputs(inputs: Seq[DataType]): UserDefinedFunction = copy(inputs = inputs.map(dt => dt: TypeSpec))

  def withOutput(dt: DataType): UserDefinedFunction = copy(output = dt)
  def withOutput(schema: StructType): UserDefinedFunction = copy(output = schema)
}
