package ml.combust.mleap.json

import java.nio.charset.Charset

import JsonSupport._
import ml.combust.mleap.runtime.frame.LeapFrame
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameWriter}
import spray.json._

import scala.util.Try

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultFrameWriter[LF <: LeapFrame[LF]](frame: LF) extends FrameWriter {
  override def toBytes(charset: Charset = BuiltinFormats.charset): Try[Array[Byte]] = {
    Try(frame.toJson.prettyPrint.getBytes(charset))
  }
}
