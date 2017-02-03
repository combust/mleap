package ml.combust.mleap.serving.marshalling

import ml.combust.mleap.serving.domain.v1.{LoadModelRequest, LoadModelResponse, UnloadModelRequest, UnloadModelResponse}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

/**
  * Created by hollinwilkins on 1/30/17.
  */
trait JsonSupport {
  implicit val mleapLoadModelRequestFormat: RootJsonFormat[LoadModelRequest] = jsonFormat1(LoadModelRequest)
  implicit val mleapLoadModelResponseFormat: RootJsonFormat[LoadModelResponse] = jsonFormat0(LoadModelResponse)

  implicit val mleapUnloadModelRequestFormat: RootJsonFormat[UnloadModelRequest] = jsonFormat0(UnloadModelRequest)
  implicit val mleapUnloadModelResponseFormat: RootJsonFormat[UnloadModelResponse] = jsonFormat0(UnloadModelResponse)
}
object JsonSupport extends JsonSupport
