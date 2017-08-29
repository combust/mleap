package ml.combust.mleap.runtime

import ml.combust.bundle.dsl.Bundle
import ml.combust.bundle.{BundleFile, BundleWriter}
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.converter.LeapFrameConverter
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, RowReader, RowWriter}
import ml.combust.mleap.runtime.transformer.Transformer

import scala.reflect.runtime.universe._
import scala.util.Try

/** Object for support classes for easily working with Bundle.ML and DefaultLeapFrame.
  */
object MleapSupport {
  implicit class MleapTransformerOps(transformer: Transformer) {
    def writeBundle: BundleWriter[MleapContext, Transformer] = BundleWriter(transformer)
  }

  implicit class MleapBundleFileOps(file: BundleFile) {
    def loadMleapBundle()
                       (implicit context: MleapContext): Try[Bundle[Transformer]] = file.load()
  }

  implicit class MleapCaseClassOps[T <: Product](data: T)(implicit tag: TypeTag[T]) {
    def toLeapFrame: DefaultLeapFrame = LeapFrameConverter.convert(data)
  }

  implicit class MleapCaseClassSeqOps[T <: Product](data: Seq[T])(implicit tag: TypeTag[T]) {
    def toLeapFrame: DefaultLeapFrame = LeapFrameConverter.convert(data)
  }

  implicit class MleapLeapFrameOps(frame: DefaultLeapFrame) {
    def to[T <: Product](implicit tag: TypeTag[T]): Seq[T] =
      LeapFrameConverter.convert(frame)
  }

  implicit class StructTypeOps(schema: StructType) {
    def rowReader(format: String = BuiltinFormats.json): RowReader = RowReader(schema, format)
    def rowWriter(format: String = BuiltinFormats.json): RowWriter = RowWriter(schema, format)
  }
}
