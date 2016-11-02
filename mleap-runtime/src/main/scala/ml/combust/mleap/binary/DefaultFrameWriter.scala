package ml.combust.mleap.binary

import java.io.{ByteArrayOutputStream, DataOutputStream}

import ml.combust.mleap.runtime.LeapFrame
import ml.combust.mleap.runtime.serialization.{Defaults, FrameWriter}
import ml.combust.mleap.json.JsonSupport._
import spray.json._
import resource._

/**
  * Created by hollinwilkins on 11/2/16.
  */
class DefaultFrameWriter extends FrameWriter {
  override def toBytes[LF <: LeapFrame[LF]](frame: LF): Array[Byte] = {
    (for(out <- managed(new ByteArrayOutputStream())) yield {
      val serializers = frame.schema.fields.map(_.dataType).map(ValueSerializer.serializerForDataType)
      val dout = new DataOutputStream(out)
      val schemaBytes = frame.schema.toJson.prettyPrint.getBytes(Defaults.charset)
      dout.writeInt(schemaBytes.length)
      dout.write(schemaBytes)
      dout.writeInt(frame.dataset.size)

      for(row <- frame.dataset) {
        var i = 0
        for(s <- serializers) {
          s.write(row(i), dout)
          i = i + 1
        }
      }

      dout.flush()
      out.toByteArray
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(bytes) => bytes
    }
  }
}
