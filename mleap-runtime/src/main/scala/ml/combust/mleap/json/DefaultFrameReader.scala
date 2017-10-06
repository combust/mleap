package ml.combust.mleap.json

import java.nio.charset.Charset

import ml.combust.mleap.runtime.MleapContext
import JsonSupport._
import ml.combust.mleap.core.frame.DefaultLeapFrame
import ml.combust.mleap.core.serialization.{BuiltinFormats, FrameReader}
import spray.json._

import scala.util.Try

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultFrameReader extends FrameReader {
  override def fromBytes(bytes: Array[Byte], charset: Charset = BuiltinFormats.charset): Try[DefaultLeapFrame] = {
    Try(new String(bytes, charset).trim.parseJson.convertTo[DefaultLeapFrame])
  }
}
