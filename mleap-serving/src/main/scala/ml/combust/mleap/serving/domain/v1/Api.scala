package ml.combust.mleap.serving.domain.v1

import ml.combust.mleap.core.frame.DefaultLeapFrame

/**
  * Created by hollinwilkins on 1/30/17.
  */
case class LoadModelRequest(path: Option[String] = None) {
  def withPath(value: String): LoadModelRequest = copy(path = Some(value))
}
case class LoadModelResponse()

case class UnloadModelRequest()
case class UnloadModelResponse()

case class TransformRequest(frame: Option[DefaultLeapFrame]) {
  def withFrame(value: DefaultLeapFrame): TransformRequest = copy(frame = Some(value))
}
case class TransformResponse(frame: DefaultLeapFrame)
