package ml.combust.mleap.runtime.serialization.json

import java.nio.charset.Charset

import ml.combust.mleap.runtime.{DefaultLeapFrame, LeapFrame, MleapContext}
import ml.combust.mleap.runtime.serialization.{FrameSerializer, FrameSerializerContext, RowSerializer}
import ml.combust.mleap.runtime.types.StructType
import JsonSupport._
import spray.json._

/**
  * Created by hollinwilkins on 10/31/16.
  */
object DefaultFrameSerializer {
  val charset = Charset.forName("UTF-8")
}

class DefaultFrameSerializer(override val serializerContext: FrameSerializerContext) extends FrameSerializer {
  implicit val context: MleapContext = serializerContext.context

  override def toBytes[LF <: LeapFrame[LF]](frame: LF): Array[Byte] = {
    frame.toJson.prettyPrint.getBytes(DefaultFrameSerializer.charset)
  }

  override def fromBytes(bytes: Array[Byte]): DefaultLeapFrame = {
    new String(bytes, DefaultFrameSerializer.charset).parseJson.convertTo[DefaultLeapFrame]
  }

  override def rowSerializer(schema: StructType): RowSerializer = JsonRowSerializer(schema)
}
