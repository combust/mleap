package ml.combust.mleap.runtime.serialization

import java.nio.charset.Charset

import ml.combust.bundle.util.ClassLoaderUtil
import ml.combust.mleap.runtime.types.StructType
import ml.combust.mleap.runtime.Row

/**
  * Created by hollinwilkins on 11/1/16.
  */
object RowReader {
  def apply(schema: StructType,
            format: String = BuiltinFormats.json,
            classLoader: Option[ClassLoader] = None): RowReader = {
    ClassLoaderUtil.resolveClassLoader(classLoader).
      loadClass(s"$format.DefaultRowReader").
      getConstructor(classOf[StructType]).
      newInstance(schema).
      asInstanceOf[RowReader]
  }
}

trait RowReader {
  val schema: StructType

  def fromBytes(bytes: Array[Byte], charset: Charset = BuiltinFormats.charset): Row
}
