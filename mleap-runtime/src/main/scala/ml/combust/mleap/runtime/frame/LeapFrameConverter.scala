package ml.combust.mleap.runtime.frame

import ml.combust.mleap.core.reflection.MleapReflection._
import ml.combust.mleap.core.types.{StructField, StructType}

import scala.reflect.runtime.universe._

trait LeapFrameConverter {

  def convert[T <: Product](data: T)(implicit tag: TypeTag[T]): DefaultLeapFrame = {
    val params = extractConstructorParameters[T]
    val structType = StructType(params.map(p => StructField(p._1, p._2))).get

    DefaultLeapFrame(structType, Seq(ArrayRow(data.productIterator.toList)))
  }

  def convert[T <: Product](data: Seq[T])(implicit tag: TypeTag[T]): DefaultLeapFrame = {
    val params = extractConstructorParameters[T]
    val structType = StructType(params.map(p => StructField(p._1, p._2))).get
    val rows = data.map(row => ArrayRow(row.productIterator.toList))

    DefaultLeapFrame(structType, rows)
  }

  def convert[LF <: LeapFrame[LF], T <: Product](frame: LF)(implicit tag: TypeTag[T]): Seq[T] = {
    frame.collect().map(row => newInstance[T](row.toSeq))
  }
}

object LeapFrameConverter extends LeapFrameConverter