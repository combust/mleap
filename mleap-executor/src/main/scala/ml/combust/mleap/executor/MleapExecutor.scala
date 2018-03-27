package ml.combust.mleap.executor

import java.net.URI
import java.util.concurrent.{ExecutorService, Executors}

import akka.NotUsed
import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.stream.scaladsl.Flow
import ml.combust.mleap.executor.repository.{FileRepository, Repository, RepositoryBundleLoader}
import ml.combust.mleap.executor.service.TransformService
import ml.combust.mleap.executor.stream.TransformStream
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object MleapExecutor extends ExtensionId[MleapExecutor] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): MleapExecutor = {
    new MleapExecutor()(system)
  }

  override def lookup(): ExtensionId[_ <: Extension] = MleapExecutor
}

class MleapExecutor()
                   (implicit system: ExtendedActorSystem) extends Extension {
  import system.dispatcher

  private val diskThreadPool: ExecutorService = Executors.newFixedThreadPool(8)
  private val diskEc: ExecutionContext = ExecutionContext.fromExecutor(diskThreadPool)

  private val repository: Repository = new FileRepository(true)
  private val loader: RepositoryBundleLoader = new RepositoryBundleLoader(repository, diskEc)
  private val transformService: TransformService = new TransformService(loader)(system.dispatcher, system)

  system.registerOnTermination {
    diskThreadPool.shutdown()
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

  def rowFlow[Tag](uri: URI,
                   spec: StreamRowSpec,
                   parallelism: Int = TransformStream.DEFAULT_PARALLELISM)
                  (implicit timeout: FiniteDuration): Flow[(Row, Tag), (Try[Option[Row]], Tag), NotUsed] = {
    transformService.rowFlow(uri, spec, parallelism)
  }
}
