package ml.combust.mleap.serving.marshalling

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import JsonSupport._
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.http.scaladsl.util.FastFuture
import akka.util.ByteString
import ml.combust.mleap.serving.domain.v1._

/**
  * Created by hollinwilkins on 1/30/17.
  */
trait ApiMarshalling {
  implicit val mleapLoadModelRequestEntityUnmarshaller: FromEntityUnmarshaller[LoadModelRequest] = mleapLoadModelRequestFormat
  implicit val mleapLoadModelZipRequestEntityUnmarshaller: FromEntityUnmarshaller[LoadModelZipRequest] = {
    Unmarshaller.byteStringUnmarshaller.
      andThen {
        Unmarshaller[ByteString, LoadModelZipRequest] {
          _ => bs => FastFuture.successful(LoadModelZipRequest(bs.toArray))
        }
      }.forContentTypes(ContentTypes.`application/binary`)
  }
  implicit val mleapUnloadModelRequestEntityUnmarshaller: FromEntityUnmarshaller[UnloadModelRequest] = mleapUnloadModelRequestFormat

  implicit val mleapLoadModelResponseEntityMarshaller: ToEntityMarshaller[LoadModelResponse] = mleapLoadModelResponseFormat
  implicit val mleapUnloadModelResponseEntitytMarshaller: ToEntityMarshaller[UnloadModelResponse] = mleapUnloadModelResponseFormat
}
object ApiMarshalling extends ApiMarshalling
