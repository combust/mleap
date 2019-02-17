package ml.combust.mleap.springboot

import java.util.concurrent.CompletionStage

import akka.actor.ActorSystem
import com.google.protobuf.ByteString
import ml.combust.mleap.executor._
import ml.combust.mleap.pb.TransformStatus.STATUS_ERROR
import ml.combust.mleap.pb.{BundleMeta, Mleap, Model, TransformFrameResponse}
import ml.combust.mleap.runtime.serialization.{FrameReader, FrameWriter}
import ml.combust.mleap.springboot.TypeConverters._
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation._

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success}

@RestController
@RequestMapping
class ProtobufScoringController(@Autowired val actorSystem : ActorSystem,
                                @Autowired val mleapExecutor: MleapExecutor) {

  private val executor = actorSystem.dispatcher

  @PostMapping(path = Array("/models"),
    consumes = Array("application/x-protobuf; charset=UTF-8"),
    produces = Array("application/x-protobuf; charset=UTF-8"))
  @ResponseStatus(HttpStatus.ACCEPTED)
  def loadModel(@RequestBody request: Mleap.LoadModelRequest,
                @RequestHeader(value = "timeout", defaultValue = "60000") timeout: Int) : CompletionStage[Mleap.Model] = {
    mleapExecutor
      .loadModel(javaPbToExecutorLoadModelRequest(request))(timeout)
      .map(model => Model.toJavaProto(model))(executor).toJava
  }

  @DeleteMapping(path = Array("/models/{model_name}"),
    consumes = Array("application/x-protobuf; charset=UTF-8"),
    produces = Array("application/x-protobuf; charset=UTF-8"))
  def unloadModel(@PathVariable("model_name") modelName: String,
                  @RequestHeader(value = "timeout", defaultValue = "60000") timeout: Int): CompletionStage[Mleap.Model] =
    mleapExecutor
      .unloadModel(UnloadModelRequest(modelName))(timeout)
      .map(model => Model.toJavaProto(model))(executor).toJava

  @GetMapping(path = Array("/models/{model_name}"),
    consumes = Array("application/x-protobuf; charset=UTF-8"),
    produces = Array("application/x-protobuf; charset=UTF-8"))
  def getModel(@PathVariable("model_name") modelName: String,
               @RequestHeader(value = "timeout", defaultValue = "60000") timeout: Int): CompletionStage[Mleap.Model] =
    mleapExecutor
      .getModel(GetModelRequest(modelName))(timeout)
      .map(model => Model.toJavaProto(model))(executor).toJava

  @GetMapping(path = Array("/models/{model_name}/meta"),
    consumes = Array("application/x-protobuf; charset=UTF-8"),
    produces = Array("application/x-protobuf; charset=UTF-8"))
  def getMeta(@PathVariable("model_name") modelName: String,
              @RequestHeader(value = "timeout", defaultValue = "60000") timeout: Int) : CompletionStage[Mleap.BundleMeta] =
    mleapExecutor
      .getBundleMeta(GetBundleMetaRequest(modelName))(timeout)
      .map(meta => BundleMeta.toJavaProto(meta))(executor).toJava

  @PostMapping(path = Array("/models/transform"),
    consumes = Array("application/x-protobuf; charset=UTF-8"),
    produces = Array("application/x-protobuf; charset=UTF-8"))
  def transform(@RequestBody request: Mleap.TransformFrameRequest,
                @RequestHeader(value = "timeout", defaultValue = "60000") timeout: Int) : CompletionStage[Mleap.TransformFrameResponse] = {
    FrameReader(request.getFormat).fromBytes(request.getFrame.toByteArray) match {
      case Success(frame) =>
        mleapExecutor.transform(TransformFrameRequest(request.getModelName, frame, request.getOptions))(timeout)
        .mapAll {
          case Success(resp) => resp match {
            case Success(frame) => TransformFrameResponse(tag = request.getTag,
              frame = ByteString.copyFrom(FrameWriter(frame, request.getFormat).toBytes().get))
            case Failure(ex) => handleTransformFailure(request.getTag, ex)
          }
          case Failure(ex) => handleTransformFailure(request.getTag, ex)
        }(executor)
        .map(response => TransformFrameResponse.toJavaProto(response))(executor).toJava
      case Failure(ex) => Future {
          TransformFrameResponse.toJavaProto(handleTransformFailure(request.getTag, ex))
        }(executor).toJava
    }
  }

  private def handleTransformFailure(tag: Long, ex: Throwable): TransformFrameResponse = {
    ProtobufScoringController.logger.error("Transform error due to ", ex)
    TransformFrameResponse(tag = tag, status = STATUS_ERROR,
      error = ExceptionUtils.getMessage(ex), backtrace = ExceptionUtils.getStackTrace(ex))
  }
}

object ProtobufScoringController {
  val logger = LoggerFactory.getLogger(classOf[ProtobufScoringController])
}
