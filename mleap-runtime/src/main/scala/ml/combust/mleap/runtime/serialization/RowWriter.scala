package ml.combust.mleap.runtime.serialization

import java.nio.charset.Charset

import ml.combust.mleap.ClassLoaderUtil
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.frame.Row

import scala.util.Try

/**
  * Created by hollinwilkins on 11/1/16.
  */
object RowWriter {
  def apply(schema: StructType,
            format: String = BuiltinFormats.json,
            clOption: Option[ClassLoader] = None): RowWriter = {
    val cl = clOption.getOrElse(ClassLoaderUtil.findClassLoader(classOf[RowWriter].getCanonicalName))
    cl.loadClass(s"$format.DefaultRowWriter").
      getConstructor(classOf[StructType]).
      newInstance(schema).
      asInstanceOf[RowWriter]
  }
}

trait RowWriter {
  val schema: StructType

  def toBytes(row: Row, charset: Charset = BuiltinFormats.charset): Try[Array[Byte]]
}
