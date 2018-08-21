package ml.combust.mleap.executor

import java.util.concurrent.{Executors, TimeUnit}

import akka.NotUsed
import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.stream.scaladsl.Flow
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import ml.combust.mleap.executor.repository.{Repository, RepositoryBundleLoader}
import ml.combust.mleap.executor.service.{LocalTransformService, TransformService}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

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
                   (implicit system: ExtendedActorSystem) extends Extension with TransformService {
  private val logger = Logger(classOf[MleapExecutor])

  val executorConfig: ExecutorConfig = new ExecutorConfig(tConfig.getConfig("default"))

  private val loadThreadPool = Executors.newFixedThreadPool(4)
  private val loadEc = ExecutionContext.fromExecutor(loadThreadPool)
  private val repository: Repository = Repository.fromConfig(tConfig.getConfig("repository"))
  private val loader: RepositoryBundleLoader = new RepositoryBundleLoader(repository, loadEc)
  private val transformService: LocalTransformService = new LocalTransformService(loader, executorConfig)(system)

  system.registerOnTermination {
    logger.info("Shutting down executor")

    logger.info("Shutting down bundle loader")
    loadThreadPool.shutdown()
    loadThreadPool.awaitTermination(30, TimeUnit.SECONDS)

    logger.info("Shutting down repository")
    repository.shutdown()
    repository.awaitTermination(30, TimeUnit.SECONDS)
  }

  override def getBundleMeta(request: GetBundleMetaRequest)
                            (implicit timeout: FiniteDuration): Future[BundleMeta] = {
    transformService.getBundleMeta(request)
  }

  override def getModel(request: GetModelRequest)
                       (implicit timeout: FiniteDuration): Future[Model] = {
    transformService.getModel(request)

  }

  override def loadModel(request: LoadModelRequest)
                        (implicit timeout: FiniteDuration): Future[Model] = {
    transformService.loadModel(request)
  }

  override def unloadModel(request: UnloadModelRequest)
                          (implicit timeout: FiniteDuration): Future[Model] = {
    transformService.unloadModel(request)
  }

  override def createFrameStream(request: CreateFrameStreamRequest)
                                (implicit timeout: FiniteDuration): Future[FrameStream] = {
    transformService.createFrameStream(request)
  }

  override def createRowStream(request: CreateRowStreamRequest)
                              (implicit timeout: FiniteDuration): Future[RowStream] = {
    transformService.createRowStream(request)
  }

  override def transform(request: TransformFrameRequest)
                        (implicit timeout: FiniteDuration): Future[Try[DefaultLeapFrame]] = {
    transformService.transform(request)
  }

  override def createFrameFlow[Tag](request: CreateFrameFlowRequest)
                                             (implicit timeout: FiniteDuration): Flow[(StreamTransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed] = {
    transformService.createFrameFlow(request)
  }

  override def createRowFlow[Tag](request: CreateRowFlowRequest)
                                           (implicit timeout: FiniteDuration): Flow[(StreamTransformRowRequest, Tag), (Try[Option[Row]], Tag), NotUsed] = {
    transformService.createRowFlow(request)
  }

  override def getFrameStream(request: GetFrameStreamRequest)
                             (implicit timeout: FiniteDuration): Future[FrameStream] = {
    transformService.getFrameStream(request)

  }

  override def getRowStream(request: GetRowStreamRequest)
                           (implicit timeout: FiniteDuration): Future[RowStream] = {
    transformService.getRowStream(request)
  }

  override def close(): Unit = { }
}
