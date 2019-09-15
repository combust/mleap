package ml.combust.mleap.runtime.serialization

import java.io._
import java.nio.charset.Charset

import ml.combust.mleap.ClassLoaderUtil
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import org.apache.commons.io.IOUtils
import resource._

import scala.util.Try

/**
  * Created by hollinwilkins on 11/1/16.
  */
object FrameReader {
  def apply(format: String = BuiltinFormats.json,
            clOption: Option[ClassLoader] = None): FrameReader = {
    val cl = clOption.getOrElse(ClassLoaderUtil.findClassLoader(classOf[FrameReader].getCanonicalName))
    cl.loadClass(s"$format.DefaultFrameReader").
      newInstance().
      asInstanceOf[FrameReader]
  }
}

trait FrameReader {
  def fromBytes(bytes: Array[Byte], charset: Charset = BuiltinFormats.charset): Try[DefaultLeapFrame]

  def read(file: File): Try[DefaultLeapFrame] = read(file, BuiltinFormats.charset)
  def read(file: File, charset: Charset): Try[DefaultLeapFrame] = {
    (for(in <- managed(new FileInputStream(file))) yield {
      read(in, charset)
    }).tried.flatMap(identity)
  }

  def read(in: InputStream): Try[DefaultLeapFrame] = read(in, BuiltinFormats.charset)
  def read(in: InputStream, charset: Charset): Try[DefaultLeapFrame] = {
    Try(IOUtils.toByteArray(in)).flatMap(bytes => fromBytes(bytes, charset))
  }

  }
