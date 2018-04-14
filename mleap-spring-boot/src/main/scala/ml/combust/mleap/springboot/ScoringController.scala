package ml.combust.mleap.springboot

import java.net.URI
import java.util.concurrent.CompletableFuture

import akka.actor.ActorSystem
import ml.combust.mleap.executor.MleapExecutor
import ml.combust.mleap.pb.{BundleMeta, Mleap}
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.web.bind.annotation._
import ml.combust.mleap.runtime.types.BundleTypeConverters._

import scala.compat.java8.FutureConverters._

@RestController
@RequestMapping
class ScoringController(@Autowired val mleapExecutor: MleapExecutor,
                        @Autowired val actorSystem : ActorSystem,
                        @Value("${bundleMeta.timeout}") bundleMetaTimeout: Int) {

  @GetMapping(path = Array("/bundle-meta"),
    consumes = Array("application/x-protobuf; charset=UTF-8"),
    produces = Array("application/x-protobuf; charset=UTF-8"))
  def getBundleMeta(@RequestParam uri: String) : CompletableFuture[Mleap.BundleMeta] =
    mleapExecutor.getBundleMeta(URI.create(uri), bundleMetaTimeout)
    .map { meta =>
      BundleMeta.toJavaProto(BundleMeta(Some(meta.info.asBundle),
          Some(meta.inputSchema), Some(meta.outputSchema)))
    }(actorSystem.dispatcher).toJava.toCompletableFuture

  @PostMapping(path = Array("/transform/frame"),
    consumes = Array("application/x-protobuf; charset=UTF-8"),
    produces = Array("application/x-protobuf; charset=UTF-8"))
  def transformFrame(@RequestBody transformFrameRequest: Mleap.TransformFrameRequest) : CompletableFuture[Mleap.TransformFrameResponse] = {
    //todo
    throw new NotImplementedError()
  }
}