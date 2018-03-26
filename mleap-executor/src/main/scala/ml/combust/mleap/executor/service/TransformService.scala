package ml.combust.mleap.executor.service

import java.net.URI

import akka.NotUsed
import akka.pattern.ask
import akka.actor.{ActorRef, ActorRefFactory}
import akka.stream.scaladsl.Flow
import ml.combust.mleap.executor.{BundleMeta, StreamRowSpec, TransformFrameRequest, TransformRowRequest}
import ml.combust.mleap.executor.repository.RepositoryBundleLoader
import ml.combust.mleap.executor.stream.TransformStream
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Try}

class BundleManager(manager: TransformService,
                    loader: RepositoryBundleLoader,
                    uri: URI)
                   (implicit arf: ActorRefFactory,
                    ec: ExecutionContext) {
  lazy val actor: ActorRef = arf.actorOf(BundleActor.props(manager, uri, loader.loadBundle(uri)))

  def getBundleMeta()
                   (implicit timeout: FiniteDuration): Future[BundleMeta] = {
    (actor ? BundleActor.GetBundleMeta)(timeout).mapTo[BundleMeta]
  }

  def transform(request: TransformFrameRequest)
               (implicit timeout: FiniteDuration): Future[DefaultLeapFrame] = {
    (actor ? request)(timeout).mapTo[DefaultLeapFrame]
  }

  def transformRow(request: TransformRowRequest)
                  (implicit timeout: FiniteDuration): Future[Option[Row]] = {
    (actor ? request)(timeout).mapTo[Option[Row]]
  }

  def close(): Unit = actor ! BundleActor.Shutdown
}

class TransformService(loader: RepositoryBundleLoader)
                      (implicit ec: ExecutionContext,
                       arf: ActorRefFactory) {
  private val lookup: concurrent.Map[URI, BundleManager] = concurrent.TrieMap()

  def getbundleMeta(uri: URI)
                   (implicit timeout: FiniteDuration): Future[BundleMeta] = {
    manager(uri).getBundleMeta()
  }

  def transform(uri: URI, request: TransformFrameRequest)
               (implicit timeout: FiniteDuration): Future[DefaultLeapFrame] = {
    manager(uri).transform(request)
  }

  def transformRow(uri: URI,
                   request: TransformRowRequest)
                  (implicit timeout: FiniteDuration): Future[Option[Row]] = {
    manager(uri).transformRow(request)
  }

  def rowFlow[Tag](uri: URI,
                   spec: StreamRowSpec,
                   parallelism: Int = TransformStream.DEFAULT_PARALLELISM)
                  (implicit timeout: FiniteDuration): Flow[(Row, Tag), (Try[Option[Row]], Tag), NotUsed] = {
    Flow[(Row, Tag)].mapAsyncUnordered(parallelism) {
      case (row, tag) =>
        transformRow(uri, TransformRowRequest(spec, row)).map(r => Try(r)).recover {
          case error => Failure(error)
        }.map(r => (r, tag))
    }
  }

  private def manager(uri: URI): BundleManager = {
    lookup.getOrElseUpdate(uri, new BundleManager(this, loader, uri))
  }

  def unload(uri: URI): Unit = {
    lookup.remove(uri).foreach(_.close())
  }
}
