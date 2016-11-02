package ml.combust.mleap.runtime.serialization

import java.io.{File, FileOutputStream}

import ml.combust.bundle.util.ClassLoaderUtil
import ml.combust.mleap.runtime.{DefaultLeapFrame, LeapFrame}
import resource._

/**
  * Created by hollinwilkins on 11/1/16.
  */
object FrameWriter {
  def apply(format: String = BuiltinFormats.json,
            classLoader: Option[ClassLoader] = None): FrameWriter = {
    ClassLoaderUtil.resolveClassLoader(classLoader).
      loadClass(s"$format.DefaultFrameWriter").
      newInstance().
      asInstanceOf[FrameWriter]
  }
}

trait FrameWriter {
  def toBytes[LF <: LeapFrame[LF]](frame: LF): Array[Byte]

  def write[LF <: LeapFrame[LF]](frame: LF, file: File): Unit = {
    val bytes = toBytes(frame)
    for(out <- managed(new FileOutputStream(file))) {
      out.write(bytes)
    }
  }
}
