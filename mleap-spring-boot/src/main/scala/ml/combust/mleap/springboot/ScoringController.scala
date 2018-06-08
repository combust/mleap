package ml.combust.mleap.springboot

import java.net.URI
import java.util.concurrent.CompletionStage

import akka.actor.ActorSystem
import ml.combust.mleap.executor.{MleapExecutor, TransformFrameRequest, TransformOptions}
import ml.combust.mleap.pb.{BundleMeta, Mleap, TransformFrameResponse}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation._
import ml.combust.mleap.runtime.types.BundleTypeConverters._

import scala.compat.java8.FutureConverters._
import TypeConverters._
import com.google.protobuf.ByteString
import ml.combust.mleap.pb
import ml.combust.mleap.pb.TransformStatus.STATUS_ERROR
import ml.combust.mleap.runtime.serialization.{FrameReader, FrameWriter}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.json4s.jackson.JsonMethods
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}
import scalapb.json4s.{Parser, Printer}

@RestController
@RequestMapping
class ScoringController(@Autowired val actorSystem : ActorSystem,
                        @Autowired val mleapExecutor: MleapExecutor,
                        @Autowired val jsonPrinter: Printer,
                        @Autowired val jsonParser: Parser) {

  private val executor = actorSystem.dispatcher

  @GetMapping(path = Array("/bundle-meta"), consumes = Array("application/x-protobuf; charset=UTF-8"),
    produces = Array("application/x-protobuf; charset=UTF-8"))
  def getBundleMetaProto(@RequestParam timeout: Int, @RequestParam uri: String) : CompletionStage[Mleap.BundleMeta] =
    getBundleMeta(timeout, uri).map(meta => BundleMeta.toJavaProto(meta))(executor).toJava


  @GetMapping(path = Array("/bundle-meta"), consumes = Array("application/json; charset=UTF-8"),
    produces = Array("application/json; charset=UTF-8"))
  def getBundleMetaJson(@RequestParam timeout: Int, @RequestParam uri: String) : CompletionStage[String] =
    getBundleMeta(timeout, uri).map(meta => JsonMethods.compact(jsonPrinter.toJson(meta)))(executor).toJava

  private def getBundleMeta(timeout: Int, uri: String) = {
    mleapExecutor
      .getBundleMeta(URI.create(uri), timeout)
      .map(meta => BundleMeta(Some(meta.info.asBundle), Some(meta.inputSchema), Some(meta.outputSchema)))(executor)
  }

  @PostMapping(path = Array("/transform/frame"), consumes = Array("application/x-protobuf; charset=UTF-8"),
    produces = Array("application/x-protobuf; charset=UTF-8"))
  def transformFrameProto(@RequestBody request: Mleap.TransformFrameRequest, @RequestHeader(value = "X-Timeout", defaultValue = "60000") timeout: Int) : CompletionStage[Mleap.TransformFrameResponse] = {
    transformFrame(request.getUri, request.getFormat, request.getTag, timeout, request.getFrame, request.getOptions)
      .map(resp => TransformFrameResponse.toJavaProto(resp))(executor).toJava
  }

  @PostMapping(path = Array("/transform/frame"), consumes = Array("application/json; charset=UTF-8"),
    produces = Array("application/json; charset=UTF-8"))
  def transformFrameJson(@RequestBody requestBody: String, @RequestHeader(value = "X-Timeout", defaultValue = "60000") timeout: Int) : CompletionStage[String] = {
    val request = jsonParser.fromJsonString[pb.TransformFrameRequest](requestBody)
    transformFrame(request.uri, request.format, request.tag, timeout, request.frame, request.getOptions)
      .map(resp => JsonMethods.compact(jsonPrinter.toJson(resp)))(executor).toJava
  }

  private def transformFrame(uri: String, format: String, tag: ByteString, timeout: Long, frame: ByteString, options: TransformOptions) = {
    mleapExecutor.transform(URI.create(uri),
      TransformFrameRequest(FrameReader(format).fromBytes(frame.toByteArray), options))(timeout)
      .mapAll {
        case Success(resp) => TransformFrameResponse(tag = tag,
          frame = ByteString.copyFrom(FrameWriter(resp, format).toBytes().get))
        case Failure(ex) => {
          ScoringController.logger.error("Transform error due to ", ex)
          TransformFrameResponse(tag = tag, status = STATUS_ERROR,
            error = ExceptionUtils.getMessage(ex), backtrace = ExceptionUtils.getStackTrace(ex))
        }
      }(executor)
  }
}

object ScoringController {
  val logger = LoggerFactory.getLogger(classOf[GlobalExceptionHandler])
}