package ml.combust.mleap.runtime.serialization

import java.io.{File, FileOutputStream, OutputStream}
import java.nio.charset.Charset

import ml.combust.mleap.ClassLoaderUtil
import ml.combust.mleap.runtime.frame.LeapFrame
import scala.util.Using

import scala.reflect.ClassTag
import scala.util.Try

/**
  * Created by hollinwilkins on 11/1/16.
  */
object FrameWriter {
  def apply[LF <: LeapFrame[LF]](frame: LF,
                                 format: String = BuiltinFormats.json,
                                 clOption: Option[ClassLoader] = None)
                                (implicit ct: ClassTag[LF]): FrameWriter = {
    val cl = clOption.getOrElse(ClassLoaderUtil.findClassLoader(classOf[FrameWriter].getCanonicalName))
    cl.loadClass(s"$format.DefaultFrameWriter").
      getConstructor(classOf[LeapFrame[_]]).
      newInstance(frame).
      asInstanceOf[FrameWriter]
  }
}

trait FrameWriter {
  def toBytes(charset: Charset = BuiltinFormats.charset): Try[Array[Byte]]

  def save(file: File): Try[Any] = save(file, BuiltinFormats.charset)
  def save(file: File, charset: Charset = BuiltinFormats.charset): Try[Any] = {
    Using(new FileOutputStream(file)) { out =>
      save(out, charset)
    }.flatten
  }

  def save(out: OutputStream): Try[Any] = save(out, BuiltinFormats.charset)
  def save(out: OutputStream, charset: Charset): Try[Any] = {
    toBytes(charset).map(out.write)
  }
}
