package ml.combust.mleap.serving.marshalling

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import JsonSupport._
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import ml.combust.mleap.serving.domain.v1.{LoadModelRequest, LoadModelResponse, UnloadModelRequest, UnloadModelResponse}

/**
  * Created by hollinwilkins on 1/30/17.
  */
trait ApiMarshalling {
  implicit val mleapLoadModelRequestEntityUnmarshaller: FromEntityUnmarshaller[LoadModelRequest] = mleapLoadModelRequestFormat
  implicit val mleapUnloadModelRequestEntityUnmarshaller: FromEntityUnmarshaller[UnloadModelRequest] = mleapUnloadModelRequestFormat

  implicit val mleapLoadModelResponseEntityMarshaller: ToEntityMarshaller[LoadModelResponse] = mleapLoadModelResponseFormat
  implicit val mleapUnloadModelResponseEntitytMarshaller: ToEntityMarshaller[UnloadModelResponse] = mleapUnloadModelResponseFormat
  implicit val mleapLoadModelRequestEntityMarshaller: ToEntityMarshaller[LoadModelRequest] = mleapLoadModelRequestFormat
}
object ApiMarshalling extends ApiMarshalling
