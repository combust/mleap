package ml.combust.mleap.executor.service

import java.net.URI
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.pattern.ask
import akka.actor.{ActorRef, ActorRefFactory, ActorSystem}
import akka.stream.javadsl
import akka.stream.scaladsl.{Flow, Keep}
import ml.combust.mleap.executor.{BundleMeta, StreamRowSpec, TransformFrameRequest, TransformRowRequest}
import ml.combust.mleap.executor.repository.RepositoryBundleLoader
import ml.combust.mleap.executor.stream.TransformStream
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Try}

class BundleManager(manager: TransformService,
                    streams: StreamLookup,
                    loader: RepositoryBundleLoader,
                    uri: URI)
                   (implicit arf: ActorRefFactory,
                    ec: ExecutionContext) {
  lazy val actor: ActorRef = arf.actorOf(BundleActor.props(manager, uri, streams, loader.loadBundle(uri)))

  def getBundleMeta()
                   (implicit timeout: FiniteDuration): Future[BundleMeta] = {
    (actor ? BundleActor.GetBundleMeta)(timeout).mapTo[BundleMeta]
  }

  def transform(request: TransformFrameRequest)
               (implicit timeout: FiniteDuration): Future[DefaultLeapFrame] = {
    (actor ? request)(timeout).mapTo[DefaultLeapFrame]
  }

  def transformRow(request: TransformRowRequest)
                  (implicit timeout: FiniteDuration): Future[Try[Option[Row]]] = {
    (actor ? request)(timeout).mapTo[Try[Option[Row]]]
  }

  def close(): Unit = actor ! BundleActor.Shutdown
}

class StreamLookup() {
  private val streams: concurrent.Map[UUID, StreamRowSpec] = concurrent.TrieMap()

  def get(id: UUID): Option[StreamRowSpec] = streams.get(id)
  def put(id: UUID, spec: StreamRowSpec): Option[StreamRowSpec] = streams.putIfAbsent(id, spec)
  def remove(id: UUID): Option[StreamRowSpec] = streams.remove(id)
}

class TransformService(loader: RepositoryBundleLoader)
                      (implicit ec: ExecutionContext,
                       arf: ActorRefFactory) {
  private val streams: StreamLookup = new StreamLookup
  private val lookup: concurrent.Map[URI, BundleManager] = concurrent.TrieMap()

  def this(loader: RepositoryBundleLoader,
           system: ActorSystem) = {
    this(loader)(system.dispatcher, system)
  }

  def getBundleMeta(uri: URI)
                   (implicit timeout: FiniteDuration): Future[BundleMeta] = {
    manager(uri).getBundleMeta()
  }

  def getBundleMeta(uri: URI, timeout: Int): Future[BundleMeta] = {
    getBundleMeta(uri)(FiniteDuration(timeout, TimeUnit.MILLISECONDS))
  }

  def transform(uri: URI, request: TransformFrameRequest)
               (implicit timeout: FiniteDuration): Future[DefaultLeapFrame] = {
    manager(uri).transform(request)
  }

  def transform(uri: URI,
                request: TransformFrameRequest,
                timeout: Int): Future[DefaultLeapFrame] = {
    transform(uri, request)(FiniteDuration(timeout, TimeUnit.MILLISECONDS))
  }

  private def transformRow(uri: URI,
                           request: TransformRowRequest)
                          (implicit timeout: FiniteDuration): Future[Try[Option[Row]]] = {
    manager(uri).transformRow(request)
  }

  def rowFlow[Tag](uri: URI,
                   spec: StreamRowSpec,
                   parallelism: Int = TransformStream.DEFAULT_PARALLELISM)
                  (implicit timeout: FiniteDuration): Flow[(Try[Row], Tag), (Try[Option[Row]], Tag), NotUsed] = {
    val id = UUID.randomUUID()

    Flow[(Try[Row], Tag)].mapAsyncUnordered(parallelism) {
      case (row, tag) =>
        transformRow(uri, TransformRowRequest(id, row)).recover {
          case error => Failure(error)
        }.map(r => (r, tag))
    }.watchTermination()(Keep.right).mapMaterializedValue {
      done =>
        streams.put(id, spec)
        done.onComplete(_ => streams.remove(id))
        NotUsed
    }
  }

  def javaRowFlow[Tag](uri: URI,
                       spec: StreamRowSpec,
                       timeout: Int,
                       parallelism: Int = TransformStream.DEFAULT_PARALLELISM): javadsl.Flow[(Try[Row], Tag), (Try[Option[Row]], Tag), NotUsed] = {
    rowFlow(uri, spec, parallelism)(FiniteDuration(timeout, TimeUnit.MILLISECONDS)).asJava
  }

  private def manager(uri: URI): BundleManager = {
    lookup.getOrElseUpdate(uri, new BundleManager(this, streams, loader, uri))
  }

  def unload(uri: URI): Unit = {
    lookup.remove(uri).foreach(_.close())
  }
}
