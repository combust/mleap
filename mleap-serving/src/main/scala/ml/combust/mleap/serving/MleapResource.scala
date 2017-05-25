package ml.combust.mleap.serving

import akka.actor.ActorSystem
import akka.event.Logging.LogLevel
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, RouteResult}
import akka.http.scaladsl.server.directives.LoggingMagnet
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import ml.combust.mleap.runtime.DefaultLeapFrame
import ml.combust.mleap.serving.domain.v1._
import ml.combust.mleap.serving.marshalling.ApiMarshalling._
import ml.combust.mleap.serving.marshalling.LeapFrameMarshalling._
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._

/**
  * Created by hollinwilkins on 1/30/17.
  */
class MleapResource(service: MleapService)
                   (implicit system: ActorSystem) {
  private def recordLog(logger: LoggingAdapter, level: LogLevel)(req: HttpRequest)(res: RouteResult): Unit = res match {
    case RouteResult.Complete(x) =>
      logger.log(level, logger.format("status={} scheme=\"{}\" path=\"{}\" method=\"{}\"",
        x.status.intValue(), req.uri.scheme, req.uri.path, req.method.value))
    case RouteResult.Rejected(rejections) => // no logging required
  }

  def errorHandler(logger: LoggingAdapter): ExceptionHandler = ExceptionHandler {
    case e: Throwable =>
      logger.error(e, "error with request")
      complete((StatusCodes.InternalServerError, e))
  }

  val logger = Logging.getLogger(system.eventStream, classOf[MleapResource])
  val corsSettings = CorsSettings.defaultSettings

  val routes = handleExceptions(errorHandler(logger)) {
    withLog(logger) {
      logRequestResult(LoggingMagnet(recordLog(_, Logging.InfoLevel))) {
        cors(corsSettings) {
          path("model") {
            put {
              entity(as[LoadModelRequest]) {
                request =>
                  complete(service.loadModel(request))
              }
            } ~ delete {
              complete(service.unloadModel(UnloadModelRequest()))
            } ~ get {
              complete(service.getSchema())
            } ~ options {
              complete("ok")
            }
          } ~ path("transform") {
            post {
              entity(as[DefaultLeapFrame]) {
                frame => complete(service.transform(frame))
              }
            } ~ options {
              complete("ok")
            }
          }
        }
      }
    }
  }
}
