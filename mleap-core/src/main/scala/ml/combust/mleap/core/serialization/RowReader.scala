package ml.combust.mleap.core.serialization

import java.nio.charset.Charset

import ml.combust.mleap.ClassLoaderUtil
import ml.combust.mleap.core.frame.Row
import ml.combust.mleap.core.types.StructType

import scala.util.Try

/**
  * Created by hollinwilkins on 11/1/16.
  */
object RowReader {
  def apply(schema: StructType,
            format: String = BuiltinFormats.json,
            clOption: Option[ClassLoader] = None): RowReader = {
    val cl = clOption.getOrElse(ClassLoaderUtil.findClassLoader(classOf[RowReader].getCanonicalName))
    cl.loadClass(s"$format.DefaultRowReader").
      getConstructor(classOf[StructType]).
      newInstance(schema).
      asInstanceOf[RowReader]
  }
}

trait RowReader {
  val schema: StructType

  def fromBytes(bytes: Array[Byte], charset: Charset = BuiltinFormats.charset): Try[Row]
}
