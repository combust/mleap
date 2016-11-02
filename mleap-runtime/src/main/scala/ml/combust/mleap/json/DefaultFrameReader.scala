package ml.combust.mleap.json

import java.nio.charset.Charset

import ml.combust.mleap.runtime.{DefaultLeapFrame, MleapContext}
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameReader}
import JsonSupport._
import spray.json._

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultFrameReader extends FrameReader {
  override def fromBytes(bytes: Array[Byte], charset: Charset = BuiltinFormats.charset)
                        (implicit context: MleapContext): DefaultLeapFrame = {
    new String(bytes, charset).parseJson.convertTo[DefaultLeapFrame]
  }
}
