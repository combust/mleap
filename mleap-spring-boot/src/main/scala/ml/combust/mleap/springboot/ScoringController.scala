package ml.combust.mleap.springboot

import java.net.URI
import java.util.concurrent.CompletableFuture

import akka.actor.ActorSystem
import ml.combust.mleap.executor.{MleapExecutor, TransformFrameRequest}
import ml.combust.mleap.pb.{BundleMeta, Mleap, TransformFrameResponse}
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
  def getBundleMeta(@RequestParam uri: String) : CompletableFuture[Mleap.BundleMeta] =
    executor.getBundleMeta(URI.create(uri), bundleMetaTimeout)
    .map(meta => BundleMeta.toJavaProto(
      BundleMeta(Some(meta.info.asBundle), Some(meta.inputSchema), Some(meta.outputSchema))))(actorSystem.dispatcher)
    .toJava.toCompletableFuture

  @PostMapping(path = Array("/transform/frame"),
    consumes = Array("application/x-protobuf; charset=UTF-8"),
    produces = Array("application/x-protobuf; charset=UTF-8"))
  def transformFrame(@RequestBody rawRequest: Mleap.TransformFrameRequest) : CompletableFuture[Mleap.TransformFrameResponse] = {
    val format = rawRequest.getFormat
    val executorRequest = TransformFrameRequest(
      FrameReader(format).fromBytes(rawRequest.getFrame.toByteArray), rawRequest.getOptions)

    executor.transform(URI.create(rawRequest.getUri), executorRequest)(rawRequest.getTimeout)
        .map(r => TransformFrameResponse(tag = rawRequest.getTag,
          frame = ByteString.copyFrom(FrameWriter(r, format).toBytes().get)))(actorSystem.dispatcher)
        .map(executorResponse =>TransformFrameResponse.toJavaProto(executorResponse))(actorSystem.dispatcher)
      .toJava.toCompletableFuture
  }
}