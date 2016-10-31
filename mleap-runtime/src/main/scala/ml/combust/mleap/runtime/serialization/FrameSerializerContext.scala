package ml.combust.mleap.runtime.serialization

import java.io.File

import ml.combust.mleap.runtime.types.StructType
import ml.combust.mleap.runtime.{DefaultLeapFrame, LeapFrame, MleapContext}

/**
  * Created by hollinwilkins on 10/31/16.
  */
case class FrameSerializerContext(format: String,
                                  _classLoader: Option[ClassLoader] = None,
                                  _options: Map[String, String] = Map())
                                 (implicit val context: MleapContext) {
  def classLoader(classLoader: ClassLoader): FrameSerializerContext = copy(_classLoader = Some(classLoader))
  def options(options: Map[String, String]): FrameSerializerContext = copy(_options = this._options ++ options)
  def option(name: String, value: String): FrameSerializerContext = copy(_options = _options + (name -> value))

  def toBytes[LF <: LeapFrame[LF]](frame: LF): Array[Byte] = frameSerializer.toBytes(frame)
  def fromBytes(bytes: Array[Byte]): DefaultLeapFrame = frameSerializer.fromBytes(bytes)

  def write[LF <: LeapFrame[LF]](frame: LF, file: File): Unit = frameSerializer.write(frame, file)
  def read(file: File): DefaultLeapFrame = frameSerializer.read(file)

  def rowSerializer(schema: StructType): RowSerializer = frameSerializer.rowSerializer(schema)

  def frameSerializer: FrameSerializer = resolveClassLoader().loadClass(format).
    getConstructor(classOf[FrameSerializerContext]).
    newInstance(this).
    asInstanceOf[FrameSerializer]

  def resolveClassLoader(): ClassLoader = _classLoader.getOrElse(context.classLoader)
}
