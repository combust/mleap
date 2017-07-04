package ml.combust.mleap.runtime.function

import ml.combust.mleap.core.types.{DataType, StructType}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.core.reflection.MleapReflection._

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

object TypeSpec {
  implicit def apply(dt: DataType): DataTypeSpec = DataTypeSpec(dt)
  implicit def apply(schema: StructType): SchemaSpec = SchemaSpec(schema)
}
sealed trait TypeSpec
case class DataTypeSpec(dt: DataType) extends TypeSpec

object SchemaSpec {
  def apply(schema: StructType): SchemaSpec = SchemaSpec(schema.fields.map(_.dataType))
}
case class SchemaSpec(dts: Seq[DataType]) extends TypeSpec

/** Companion object for creating user defined functions. */
object UserDefinedFunction {
  def apply(f: AnyRef,
            output: DataType,
            inputs: Seq[TypeSpec]): UserDefinedFunction = {
    UserDefinedFunction(f, Seq(output), inputs)
  }

  def apply(f: AnyRef,
            outputs: Seq[DataType],
            input0: DataType,
            inputs: DataType *): UserDefinedFunction = {
    UserDefinedFunction(f, outputs, (input0: TypeSpec) +: inputs.map(i => i: TypeSpec))
  }

  def apply(f: AnyRef,
            output: DataType,
            input0: DataType,
            inputs: DataType *): UserDefinedFunction = {
    UserDefinedFunction(f, Seq(output), input0, inputs: _*)
  }

  implicit def function0[RT: TypeTag](f: () => RT)
                                     (implicit context: MleapContext): UserDefinedFunction = {
    UserDefinedFunction(f, Seq(dataType[RT]), Seq())
  }

  implicit def function1[RT: TypeTag, T1: TypeTag](f: (T1) => RT)
                                                  (implicit context: MleapContext): UserDefinedFunction = {
    UserDefinedFunction(f, dataType[RT], dataType[T1])
  }

  implicit def function2[RT: TypeTag, T1: TypeTag, T2: TypeTag](f: (T1, T2) => RT)
                                                               (implicit context: MleapContext): UserDefinedFunction = {
    UserDefinedFunction(f, dataType[RT], dataType[T1], dataType[T2])
  }

  implicit def function3[RT: TypeTag, T1: TypeTag, T2: TypeTag, T3: TypeTag](f: (T1, T2, T3) => RT)
                                                                            (implicit context: MleapContext): UserDefinedFunction = {
    UserDefinedFunction(f, dataType[RT], dataType[T1], dataType[T2], dataType[T3])
  }

  implicit def function4[RT: TypeTag, T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag](f: (T1, T2, T3, T4) => RT)
                                                                                         (implicit context: MleapContext): UserDefinedFunction = {
    UserDefinedFunction(f, dataType[RT], dataType[T1], dataType[T2], dataType[T3], dataType[T4])
  }

  implicit def function5[RT: TypeTag, T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag, T5: TypeTag](f: (T1, T2, T3, T4, T5) => RT)
                                                                                                      (implicit context: MleapContext): UserDefinedFunction = {
    UserDefinedFunction(f, dataType[RT], dataType[T1], dataType[T2], dataType[T3], dataType[T4], dataType[T5])
  }
}

case class UserDefinedFunction(f: AnyRef,
                               outputs: Seq[DataType],
                               inputs: Seq[TypeSpec]) {
  def withInputs(inputs: Seq[TypeSpec]): UserDefinedFunction = copy(inputs = inputs)
  def withInputs(schema: StructType): UserDefinedFunction = withDataTypeInputs(schema.fields.map(_.dataType))
  def withDataTypeInputs(inputs: Seq[DataType]): UserDefinedFunction = copy(inputs = inputs.map(dt => dt: TypeSpec))

  def withOutput(dt: DataType): UserDefinedFunction = copy(outputs = Seq(dt))
  def withOutput(schema: StructType): UserDefinedFunction = copy(outputs = schema.fields.map(_.dataType))
}
