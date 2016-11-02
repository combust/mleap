package ml.combust.mleap.runtime.serialization

import java.io.{DataInputStream, File, FileInputStream}

import ml.combust.bundle.util.ClassLoaderUtil
import ml.combust.mleap.runtime.{DefaultLeapFrame, MleapContext}
import resource._

/**
  * Created by hollinwilkins on 11/1/16.
  */
object FrameReader {
  def apply(format: String = BuiltinFormats.json,
            classLoader: Option[ClassLoader] = None): FrameReader = {
    ClassLoaderUtil.resolveClassLoader(classLoader).
      loadClass(s"$format.DefaultFrameReader").
      newInstance().
      asInstanceOf[FrameReader]
  }
}

trait FrameReader {
  def fromBytes(bytes: Array[Byte])
               (implicit context: MleapContext): DefaultLeapFrame

  def read(file: File)
          (implicit context: MleapContext): DefaultLeapFrame = {
    (for(in <- managed(new DataInputStream(new FileInputStream(file)))) yield {
      val bytes = new Array[Byte](file.length().toInt)
      in.readFully(bytes)
      fromBytes(bytes)
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(bm) => bm
    }
  }
}
