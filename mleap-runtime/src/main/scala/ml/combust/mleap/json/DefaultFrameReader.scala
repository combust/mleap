package ml.combust.mleap.json

import ml.combust.mleap.runtime.{DefaultLeapFrame, MleapContext}
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameReader}
import JsonSupport._
import spray.json._

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultFrameReader extends FrameReader {
  override def fromBytes(bytes: Array[Byte])
                        (implicit context: MleapContext): DefaultLeapFrame = {
    new String(bytes, BuiltinFormats.charset).parseJson.convertTo[DefaultLeapFrame]
  }
}
