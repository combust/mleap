package ml.combust.mleap.runtime.serialization

import java.nio.charset.Charset

import ml.combust.bundle.util.ClassLoaderUtil
import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.types.StructType

/**
  * Created by hollinwilkins on 11/1/16.
  */
object RowWriter {
  def apply(schema: StructType,
            format: String = BuiltinFormats.json,
            classLoader: Option[ClassLoader] = None): RowWriter = {
    ClassLoaderUtil.resolveClassLoader(classLoader).
      loadClass(s"$format.DefaultRowWriter").
      getConstructor(classOf[StructType]).
      newInstance(schema).
      asInstanceOf[RowWriter]
  }
}

trait RowWriter {
  val schema: StructType

  def toBytes(row: Row, charset: Charset = BuiltinFormats.charset): Array[Byte]
}
