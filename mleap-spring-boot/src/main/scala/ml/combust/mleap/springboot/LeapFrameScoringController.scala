package ml.combust.mleap.springboot

import java.util.concurrent.CompletionStage

import akka.actor.ActorSystem
import ml.combust.mleap.executor.{MleapExecutor, TransformFrameRequest}
import ml.combust.mleap.pb.ErrorTransformResponse
import ml.combust.mleap.pb.TransformStatus.STATUS_ERROR
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameReader, FrameWriter}
import ml.combust.mleap.springboot.TypeConverters._
import org.apache.commons.lang3.exception.ExceptionUtils
import org.json4s.jackson.JsonMethods
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation._

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scalapb.json4s.{Parser, Printer}

@RestController
@RequestMapping
class LeapFrameScoringController(@Autowired val actorSystem : ActorSystem,
                                 @Autowired val mleapExecutor: MleapExecutor,
                                 @Autowired val jsonPrinter: Printer,
                                 @Autowired val jsonParser: Parser) {

  private val executor = actorSystem.dispatcher

  @PostMapping(path = Array("/models/{model_name}/transform"),
    consumes = Array("application/json; charset=UTF-8"),
    produces = Array("application/json; charset=UTF-8"))
  def transformJson(@RequestBody body: Array[Byte],
                @PathVariable("model_name") modelName: String,
                @RequestHeader(value = "timeout", defaultValue = "60000") timeout: Int) : CompletionStage[_] = {
    FrameReader(BuiltinFormats.json).fromBytes(body) match {
      case Success(frame) => mleapExecutor.transform(TransformFrameRequest(modelName, frame, None))(timeout)
        .mapAll {
          case Success(resp) => resp match {
            case Success(frame) => FrameWriter(frame, BuiltinFormats.json).toBytes().get
            case Failure(ex) => JsonMethods.compact(jsonPrinter.toJson(handleTransformFailure(ex)))
          }
          case Failure(ex) => JsonMethods.compact(jsonPrinter.toJson(handleTransformFailure(ex)))

        }(executor)
        .toJava
      case Failure(ex) => Future {
        JsonMethods.compact(jsonPrinter.toJson(handleTransformFailure(ex)))
      }(executor).toJava
    }
  }

  @PostMapping(path = Array("/models/{model_name}/transform"),
    consumes = Array("application/x-protobuf; charset=UTF-8"),
    produces = Array("application/x-protobuf; charset=UTF-8"))
  def transformProto(@RequestBody body: Array[Byte],
                @PathVariable("model_name") modelName: String,
                @RequestHeader(value = "timeout", defaultValue = "60000") timeout: Int) : CompletionStage[_] = {
    FrameReader(BuiltinFormats.binary).fromBytes(body) match {
      case Success(frame) =>
        mleapExecutor.transform(TransformFrameRequest(modelName, frame, None))(timeout)
          .mapAll {
            case Success(resp) => resp match {
              case Success(frame) => FrameWriter(frame, BuiltinFormats.binary).toBytes().get
              case Failure(ex) => ErrorTransformResponse.toJavaProto(handleTransformFailure(ex))
            }
            case Failure(ex) => ErrorTransformResponse.toJavaProto(handleTransformFailure(ex))
          }(executor).toJava
      case Failure(ex) => Future {
        ErrorTransformResponse.toJavaProto(handleTransformFailure(ex))
      }(executor).toJava
    }
  }

  private def handleTransformFailure(ex: Throwable): ErrorTransformResponse = {
    LeapFrameScoringController.logger.error("Transform error due to ", ex)
    ErrorTransformResponse(status = STATUS_ERROR,
      error = ExceptionUtils.getMessage(ex), backtrace = ExceptionUtils.getStackTrace(ex))
  }
}

object LeapFrameScoringController {
  val logger = LoggerFactory.getLogger(classOf[LeapFrameScoringController])
}
