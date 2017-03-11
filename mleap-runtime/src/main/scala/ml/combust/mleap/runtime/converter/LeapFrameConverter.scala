package ml.combust.mleap.runtime.converter

import ml.combust.mleap.runtime._
import ml.combust.mleap.runtime.reflection.MleapReflection._
import ml.combust.mleap.runtime.types.{StructField, StructType}

import scala.reflect.runtime.universe._

trait LeapFrameConverter {

  def convert[T <: Product](data: T)(implicit tag: TypeTag[T]): DefaultLeapFrame = {
    val params = extractConstructorParameters[T]
    val structType = StructType(params.map(p => StructField(p._1, p._2))).get

    LeapFrame(structType, LocalDataset(ArrayRow(data.productIterator.toList)))
  }

  def convert[T <: Product](data: Seq[T])(implicit tag: TypeTag[T]): DefaultLeapFrame = {
    val params = extractConstructorParameters[T]
    val structType = StructType(params.map(p => StructField(p._1, p._2))).get
    val rows = data.map(row => ArrayRow(row.productIterator.toList))

    LeapFrame(structType, LocalDataset(rows))
  }
}

object LeapFrameConverter extends LeapFrameConverter