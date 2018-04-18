package ml.combust.mleap.springboot

import java.net.URI
import java.util.concurrent.CompletionStage

import akka.actor.ActorSystem
import ml.combust.mleap.executor.{MleapExecutor, TransformFrameRequest}
import ml.combust.mleap.pb.{BundleMeta, GetBundleMetaResponse, Mleap, TransformFrameResponse}
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.web.bind.annotation._
import ml.combust.mleap.runtime.types.BundleTypeConverters._

import scala.compat.java8.FutureConverters._
import TypeConverters._
import com.google.protobuf.ByteString
import ml.combust.mleap.runtime.serialization.{FrameReader, FrameWriter}

@RestController
@RequestMapping
class ScoringController(@Autowired val executor: MleapExecutor,
                        @Autowired val actorSystem : ActorSystem,
                        @Value("${bundleMeta.timeout}") bundleMetaTimeout: Int) {

  @GetMapping(path = Array("/bundle-meta"),
    consumes = Array("application/x-protobuf; charset=UTF-8"),
    produces = Array("application/x-protobuf; charset=UTF-8"))
  def getBundleMeta(@RequestParam uri: String) : CompletionStage[Mleap.GetBundleMetaResponse] =
    executor.getBundleMeta(URI.create(uri), bundleMetaTimeout)
    .map(meta =>
      GetBundleMetaResponse.toJavaProto(GetBundleMetaResponse(
        bundleMeta = Some(BundleMeta(Some(meta.info.asBundle), Some(meta.inputSchema), Some(meta.outputSchema)))
      )))(actorSystem.dispatcher)
    .toJava

  @PostMapping(path = Array("/transform/frame"),
    consumes = Array("application/x-protobuf; charset=UTF-8"),
    produces = Array("application/x-protobuf; charset=UTF-8"))
  def transformFrame(@RequestBody request: Mleap.TransformFrameRequest) : CompletionStage[Mleap.TransformFrameResponse] = {
    val format = request.getFormat
    val tag = request.getTag
    val timeout = request.getTimeout
    val options = request.getOptions

    val response = executor.transform(URI.create(request.getUri),
      TransformFrameRequest(FrameReader(format).fromBytes(request.getFrame.toByteArray), options))(timeout)
    response.map(resp => TransformFrameResponse(tag = tag,
                  frame = ByteString.copyFrom(FrameWriter(resp, format).toBytes().get)))(actorSystem.dispatcher)
            .map(resp => TransformFrameResponse.toJavaProto(resp))(actorSystem.dispatcher)
      .toJava
  }
}