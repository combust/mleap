package ml.combust.mleap.runtime.javadsl

import ml.combust.mleap.core.types.BasicType
import ml.combust.mleap.runtime.frame.ArrayRow

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by hollinwilkins on 4/21/17.
  */
class LeapFrameBuilderSupport {
  def createRowFromIterable(iterable: java.lang.Iterable[Any]): ArrayRow = {
    val values = iterable.asScala.map {
      case s: java.util.List[_] => s.asScala
      case v => v
    }.toArray

    new ArrayRow(mutable.WrappedArray.make[Any](values))
  }

  def createBoolean(): BasicType = BasicType.Boolean
  def createByte(): BasicType = BasicType.Byte
  def createShort(): BasicType = BasicType.Short
  def createInt(): BasicType = BasicType.Int
  def createLong(): BasicType = BasicType.Long
  def createFloat(): BasicType = BasicType.Float
  def createDouble(): BasicType = BasicType.Double
  def createString(): BasicType = BasicType.String
  def createByteString(): BasicType = BasicType.ByteString

  def createTensorDimensions(dims : java.util.List[Integer]): Option[Seq[Int]] = {
    Some(dims.asScala.map(_.intValue()))
  }
}
