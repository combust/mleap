package ml.combust.mleap.serving.marshalling

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model
import akka.http.scaladsl.model.{ContentType, HttpEntity, MediaType}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.util.ByteString
import ml.combust.mleap.core.serialization.{BuiltinFormats, FrameReader, FrameWriter}
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.json.JsonSupport._
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import spray.json._

import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 1/30/17.
  */
trait LeapFrameMarshalling {
  implicit val combustLeapFrameUnmarshaller: FromEntityUnmarshaller[DefaultLeapFrame] = {
    Unmarshaller.firstOf(createUnmarshaller(ContentTypes.`application/binary`),
      createUnmarshaller(ContentTypes.`application/avro`),
      createUnmarshaller(model.ContentTypes.`application/json`))
  }

  private def createUnmarshaller(contentType: ContentType): FromEntityUnmarshaller[DefaultLeapFrame] = {
    val reader = readerForMediaType(contentType.mediaType)
    Unmarshaller.byteStringUnmarshaller.mapWithCharset {
      (bytes, charset) =>
        reader.fromBytes(bytes.toArray, charset.nioCharset()).get
    }.forContentTypes(contentType)
  }

  implicit val combustStructTypeUnmarshaller: FromEntityUnmarshaller[StructType] = {
    Unmarshaller.byteStringUnmarshaller.mapWithCharset {
      (bytes, charset) =>
        new String(bytes.toArray, charset.nioCharset()).parseJson.convertTo[StructType]
    }
  }

  private def readerForMediaType(mediaType: MediaType): FrameReader = mediaType match {
    case model.MediaTypes.`application/json` => FrameReader(BuiltinFormats.json)
    case MediaTypes.`application/binary` => FrameReader(BuiltinFormats.binary)
    case MediaTypes.`application/avro` => FrameReader(BuiltinFormats.avro)
    case _ => throw new IllegalArgumentException(s"invalid media type for leap frame serialization: $mediaType")
  }

  implicit val combustStructTypeMarshaller: ToEntityMarshaller[StructType] = {
    Marshaller.byteStringMarshaller(model.ContentTypes.`application/json`).compose {
      schema: StructType => ByteString(schema.toJson.compactPrint.getBytes())
    }
  }

  implicit val combustLeapFrameMarshaller: ToEntityMarshaller[DefaultLeapFrame] = {
    Marshaller.oneOf(model.ContentTypes.`application/json`,
      ContentTypes.`application/binary`,
      ContentTypes.`application/avro`)(createMarshaller)
  }

  private def createMarshaller(contentType: ContentType): ToEntityMarshaller[DefaultLeapFrame] = {
    Marshaller.withFixedContentType(contentType) {
      frame: DefaultLeapFrame => HttpEntity.Strict(contentType, ByteString(writerForMediaType(frame, contentType.mediaType).toBytes().get))
    }
  }

  private def writerForMediaType(frame: DefaultLeapFrame, mediaType: MediaType): FrameWriter = mediaType match {
    case model.MediaTypes.`application/json` => FrameWriter(frame, BuiltinFormats.json)
    case MediaTypes.`application/binary` => FrameWriter(frame, BuiltinFormats.binary)
    case MediaTypes.`application/avro` => FrameWriter(frame, BuiltinFormats.avro)
    case _ => throw new IllegalArgumentException(s"invalid media type for leap frame serialization: $mediaType")
  }

  def leapFrameToEntity(frame: DefaultLeapFrame): HttpEntity.Strict = {
    val bytes = FrameWriter(frame, BuiltinFormats.binary).toBytes().get
    HttpEntity.Strict(ContentTypes.`application/binary`, ByteString(bytes))
  }

  def structTypeToEntity(schema: StructType): HttpEntity.Strict = {
    val bytes = schema.toJson.compactPrint.getBytes()
    HttpEntity.Strict(model.ContentTypes.`application/json`, ByteString(bytes))
  }
}
object LeapFrameMarshalling extends LeapFrameMarshalling

