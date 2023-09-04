package ml.combust.mleap.runtime.serialization

import ml.combust.mleap.ClassLoaderUtil
import ml.combust.mleap.runtime.frame.DefaultLeapFrame

import java.io._
import java.nio.charset.Charset
import java.nio.file.{Files, Path}
import scala.util.Try

/**
  * Created by hollinwilkins on 11/1/16.
  */
object FrameReader {
  def apply(format: String = BuiltinFormats.json,
            clOption: Option[ClassLoader] = None): FrameReader = {
    val cl = clOption.getOrElse(ClassLoaderUtil.findClassLoader(classOf[FrameReader].getCanonicalName))
    cl.loadClass(s"$format.DefaultFrameReader")
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[FrameReader]
  }
}

trait FrameReader {
  def fromBytes(bytes: Array[Byte], charset: Charset = BuiltinFormats.charset): Try[DefaultLeapFrame]

  def read(file: File): Try[DefaultLeapFrame] = read(file.toPath, BuiltinFormats.charset)

  def read(file: File, charset: Charset): Try[DefaultLeapFrame] = {
    read(file.toPath, charset)
  }
  def read(file: Path): Try[DefaultLeapFrame] = read(file, BuiltinFormats.charset)

  def read(file: Path, charset: Charset): Try[DefaultLeapFrame] = {
    Try(Files.readAllBytes(file)).flatMap(bytes => fromBytes(bytes, charset))
  }

  def read(in: InputStream): Try[DefaultLeapFrame] = read(in, BuiltinFormats.charset)
  def read(in: InputStream, charset: Charset): Try[DefaultLeapFrame] = {
    Try(in.readAllBytes()).flatMap(bytes => fromBytes(bytes, charset))
  }
}
