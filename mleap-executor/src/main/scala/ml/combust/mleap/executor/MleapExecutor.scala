package ml.combust.mleap.executor

import java.net.URI
import java.util.concurrent.{Executors, TimeUnit}

import akka.NotUsed
import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.stream.scaladsl.Flow
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import ml.combust.mleap.executor.repository.{Repository, RepositoryBundleLoader}
import ml.combust.mleap.executor.service.LocalTransformService
import ml.combust.mleap.executor.stream.TransformStream
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, RowTransformer}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object MleapExecutor extends ExtensionId[MleapExecutor] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): MleapExecutor = {
    new MleapExecutor(ConfigFactory.load().getConfig("ml.combust.mleap.executor"))(system)
  }

  override def lookup(): ExtensionId[_ <: Extension] = MleapExecutor
}

class MleapExecutor(tConfig: Config)
                   (implicit system: ExtendedActorSystem) extends Extension {
  private val logger = Logger(classOf[MleapExecutor])
  import system.dispatcher

  private val loadThreadPool = Executors.newFixedThreadPool(4)
  private val loadEc = ExecutionContext.fromExecutor(loadThreadPool)
  private val repository: Repository = Repository.fromConfig(tConfig.getConfig("repository"))
  private val loader: RepositoryBundleLoader = new RepositoryBundleLoader(repository, loadEc)
  private val transformService: LocalTransformService = new LocalTransformService(loader)(system.dispatcher, system)

  system.registerOnTermination {
    logger.info("Shutting down executor")

    logger.info("Shutting down bundle loader")
    loadThreadPool.shutdown()
    loadThreadPool.awaitTermination(30, TimeUnit.SECONDS)

    logger.info("Shutting down repository")
    repository.shutdown()
    repository.awaitTermination(30, TimeUnit.SECONDS)
  }

  def getBundleMeta(uri: URI)
                   (implicit timeout: FiniteDuration): Future[BundleMeta] = {
    transformService.getBundleMeta(uri)
  }

  def getBundleMeta(uri: URI, timeout: Int): Future[BundleMeta] = {
    transformService.getBundleMeta(uri, timeout)
  }

  def transform(uri: URI, request: TransformFrameRequest)
               (implicit timeout: FiniteDuration): Future[DefaultLeapFrame] = {
    transformService.transform(uri, request)
  }

  def transform(uri: URI,
                request: TransformFrameRequest,
                timeout: Int): Future[DefaultLeapFrame] = {
    transformService.transform(uri, request, timeout)
  }

  def frameFlow[Tag](uri: URI)
                    (implicit timeout: FiniteDuration,
                     parallelism: Parallelism): Flow[(TransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed] = {
    transformService.frameFlow(uri)
  }

  def rowFlow[Tag](uri: URI,
                   spec: StreamRowSpec)
                  (implicit timeout: FiniteDuration,
                   parallelism: Parallelism): Flow[(Try[Row], Tag), (Try[Option[Row]], Tag), Future[RowTransformer]] = {
    transformService.rowFlow(uri, spec)
  }
}
