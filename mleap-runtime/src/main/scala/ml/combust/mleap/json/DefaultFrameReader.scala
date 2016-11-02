package ml.combust.mleap.json

import ml.combust.mleap.runtime.{DefaultLeapFrame, MleapContext}
import ml.combust.mleap.runtime.serialization.{Defaults, FrameReader}
import JsonSupport._
import spray.json._

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultFrameReader(implicit override val context: MleapContext) extends FrameReader {
  override def fromBytes(bytes: Array[Byte]): DefaultLeapFrame = {
    new String(bytes, Defaults.charset).parseJson.convertTo[DefaultLeapFrame]
  }
}
