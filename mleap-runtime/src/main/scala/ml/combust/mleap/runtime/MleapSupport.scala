package ml.combust.mleap.runtime

import ml.combust.bundle.dsl.Bundle
import ml.combust.bundle.{BundleFile, BundleWriter}
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.frame.{LeapFrameConverter, Transformer}
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameWriter, RowReader, RowWriter}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Try

/** Object for support classes for easily working with Bundle.ML and DefaultLeapFrame.
  */
trait MleapSupport {
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

  implicit class MleapLeapFrameOps[LF <: LeapFrame[LF]](frame: LF) {
    def to[T <: Product](implicit tag: TypeTag[T]): Seq[T] =
      LeapFrameConverter.convert(frame)

    /** Writer for this leap frame
      *
      * @param format package with a DefaultWriter
      * @param ct class tag of this leap frame
      * @return writer for this leap frame with specified format
      */
    def writer(format: String = BuiltinFormats.json)
              (implicit ct: ClassTag[LF]): FrameWriter = FrameWriter(frame, format)
  }

  implicit class StructTypeOps(schema: StructType) {
    def rowReader(format: String = BuiltinFormats.json): RowReader = RowReader(schema, format)
    def rowWriter(format: String = BuiltinFormats.json): RowWriter = RowWriter(schema, format)
  }
}
object MleapSupport extends MleapSupport
