package ml.combust.mleap.serving

import akka.http.scaladsl.server.Directives._
import ml.combust.mleap.runtime.DefaultLeapFrame
import ml.combust.mleap.serving.domain.v1._
import ml.combust.mleap.serving.marshalling.ApiMarshalling._
import ml.combust.mleap.serving.marshalling.LeapFrameMarshalling._

/**
  * Created by hollinwilkins on 1/30/17.
  */
class MleapResource(service: MleapService) {
  val routes = path("model") {
    put {
      entity(as[LoadModelRequest]) {
        request =>
          complete(service.loadModel(request))
      }
    } ~ delete {
      complete(service.unloadModel(UnloadModelRequest()))
    }
  } ~ path("transform") {
    entity(as[DefaultLeapFrame]) {
      frame => complete(service.transform(frame))
    }
  }
}
