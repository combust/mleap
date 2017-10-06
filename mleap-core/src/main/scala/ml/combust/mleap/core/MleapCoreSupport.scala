package ml.combust.mleap.core

import ml.combust.mleap.core.converter.LeapFrameConverter
import ml.combust.mleap.core.frame.DefaultLeapFrame
import ml.combust.mleap.core.serialization.{BuiltinFormats, RowReader, RowWriter}
import ml.combust.mleap.core.types.StructType
import scala.reflect.runtime.universe._

/**
  * Created by hollinwilkins on 10/5/17.
  */
trait MleapCoreSupport {
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
object MleapCoreSupport extends MleapCoreSupport
