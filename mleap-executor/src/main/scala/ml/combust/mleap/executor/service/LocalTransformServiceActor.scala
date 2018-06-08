package ml.combust.mleap.executor.service

import java.net.URI

import akka.actor.{Actor, ActorRef, Props, Terminated}
import akka.stream.{ActorMaterializer, Materializer}
import ml.combust.mleap.executor.repository.RepositoryBundleLoader
import ml.combust.mleap.executor.{StreamConfig, StreamRowSpec, TransformFrameRequest}

object LocalTransformServiceActor {
  def props(loader: RepositoryBundleLoader): Props = {
    Props(new LocalTransformServiceActor(loader))
  }

  object Messages {
    case class GetBundleMeta(uri: URI)
    case class Transform(uri: URI, request: TransformFrameRequest)
    case class FrameFlow(uri: URI, config: StreamConfig)
    case class RowFlow(uri: URI, spec: StreamRowSpec, config: StreamConfig)
    case class Unload(uri: URI)
    case object Close
  }
}

class LocalTransformServiceActor(loader: RepositoryBundleLoader) extends Actor {
  import LocalTransformServiceActor.Messages
  private var id: Int = 0

  private implicit val materializer: Materializer = ActorMaterializer()(context.system)

  private var lookup: Map[URI, ActorRef] = Map()
  private var uriLookup: Map[ActorRef, URI] = Map()

  override def postStop(): Unit = {
    for (child <- context.children) {
      context.unwatch(child)
      context.stop(child)
    }
  }

  override def receive: Receive = {
    case request: Messages.GetBundleMeta => getBundleMeta(request)
    case request: Messages.Transform => transform(request)
    case request: Messages.FrameFlow => frameFlow(request)
    case request: Messages.RowFlow => rowFlow(request)
    case request: Messages.Unload => unload(request)
    case Messages.Close => context.stop(self)

    case Terminated(actor) => terminated(actor)
  }

  def getBundleMeta(meta: Messages.GetBundleMeta): Unit = {
    ensureActor(meta.uri).tell(meta, sender)
  }

  def transform(transform: Messages.Transform): Unit = {
    ensureActor(transform.uri).tell(transform, sender)
  }

  def frameFlow(flow: Messages.FrameFlow): Unit = {
    ensureActor(flow.uri).tell(flow, sender)
  }

  def rowFlow(flow: Messages.RowFlow): Unit = {
    ensureActor(flow.uri).tell(flow, sender)
  }

  def unload(unload: Messages.Unload): Unit = {
    ensureActor(unload.uri).tell(unload, sender)
  }

  private def ensureActor(uri: URI): ActorRef = {
    lookup.getOrElse(uri, {
      val actor = context.actorOf(BundleActor.props(uri, loader), s"bundle-$id")
      id += 1
      lookup += (uri -> actor)
      uriLookup += (actor -> uri)
      context.watch(actor)
      actor
    })
  }

  private def terminated(ref: ActorRef): Unit = {
    val uri = uriLookup(ref)
    uriLookup -= ref
    lookup -= uri
  }
}
