package ml.combust.mleap.executor.service

import java.net.URI

import akka.Done
import akka.pattern.{ask, pipe}
import akka.actor.{Actor, ActorRef, Props, ReceiveTimeout, Status, Terminated}
import akka.stream.Materializer
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.executor.{BundleMeta, ExecuteTransform, SelectMode, StreamRowSpec}
import ml.combust.mleap.executor.repository.RepositoryBundleLoader
import ml.combust.mleap.runtime.frame.{RowTransformer, Transformer}

import scala.concurrent.duration._
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

case class RequestWithSender(request: Any, sender: ActorRef)

object BundleActor {
  def props(uri: URI,
            loader: RepositoryBundleLoader)
           (implicit materializer: Materializer): Props = {
    Props(new BundleActor(uri, loader))
  }

  object Messages {
    case class Loaded(bundle: Try[Bundle[Transformer]])
  }
}

class BundleActor(uri: URI,
                  loader: RepositoryBundleLoader)
                 (implicit materializer: Materializer) extends Actor {
  import BundleActor.Messages
  import LocalTransformServiceActor.{Messages => TMessages}
  import RowFlowActor.{Messages => RFMessages}
  import FrameFlowActor.{Messages => FFMessages}
  import context.dispatcher

  // TODO: make configurable with model config API
  context.setReceiveTimeout(5.minutes)

  private var streamId: Int = 0
  private val buffer: mutable.Queue[RequestWithSender] = mutable.Queue()
  private var bundle: Option[Bundle[Transformer]] = None

  private var rowFlowLookup: Map[StreamRowSpec, ActorRef] = Map()
  private var rowFlowSpecLookup: Map[ActorRef, StreamRowSpec] = Map()

  private var frameFlowLookup: Set[ActorRef] = Set()

  private var loading: Boolean = false

  override def postStop(): Unit = {
    for (child <- context.children) {
      context.unwatch(child)
      context.stop(child)
    }
  }

  override def receive: Receive = {
    case request: TMessages.GetBundleMeta => maybeHandleRequest(RequestWithSender(request, sender))
    case request: TMessages.Transform => maybeHandleRequest(RequestWithSender(request, sender))
    case request: TMessages.FrameFlow => maybeHandleRequest(RequestWithSender(request, sender))
    case request: TMessages.RowFlow => maybeHandleRequest(RequestWithSender(request, sender))
    case request: TMessages.Unload => unload(request, sender)

    case request: Messages.Loaded => loaded(request)

    case ReceiveTimeout => receiveTimeout()
    case Terminated(actor) => terminated(actor)
  }

  def maybeHandleRequest(request: RequestWithSender): Unit = {
    if (bundle.isDefined) {
      handleRequest(request)
    } else {
      buffer += request
      maybeLoad()
    }
  }

  def handleRequest(request: RequestWithSender): Unit = request.request match {
    case r: TMessages.GetBundleMeta => getBundleMeta(r, request.sender)
    case r: TMessages.Transform => transform(r, request.sender)
    case r: TMessages.FrameFlow => frameFlow(r, request.sender)
    case r: TMessages.RowFlow => rowFlow(r, request.sender)
  }

  def maybeLoad(): Unit = {
    if (!loading) {
      loader.loadBundle(uri).map(Try(_)).recover {
        case err => Failure(err)
      }.map(Messages.Loaded).pipeTo(self)
      loading = true
    }
  }

  def loaded(loaded: Messages.Loaded): Unit = {
    loaded.bundle match {
      case Success(b) =>
        this.bundle = Some(b)
        for(r <- this.buffer.dequeueAll(_ => true)) {
          handleRequest(r)
        }
      case Failure(error) =>
        for(r <- this.buffer.dequeueAll(_ => true)) {
          r.sender ! Status.Failure(error)
        }
        context.stop(self)
    }
    loading = false
  }

  def getBundleMeta(meta: TMessages.GetBundleMeta, sender: ActorRef): Unit = {
    for (bundle <- this.bundle) {
      sender ! BundleMeta(bundle.info, bundle.root.inputSchema, bundle.root.outputSchema)
    }
  }

  def transform(transform: TMessages.Transform, sender: ActorRef): Unit = {
    for (bundle <- this.bundle;
         transformer = bundle.root;
         frame = ExecuteTransform(transformer, transform.request)) {
      frame.flatMap(Future.fromTry).pipeTo(sender)
    }
  }

  def frameFlow(flow: TMessages.FrameFlow, sender: ActorRef): Unit = {
    val actor = context.actorOf(FrameFlowActor.props(bundle.get.root, flow), s"frame-stream-$streamId")
    context.watch(actor)

    streamId += 1
    frameFlowLookup += actor

    (actor ? FFMessages.GetDone)(flow.config.initTimeout).
      mapTo[Future[Done]].
      map(done => (actor, done)).
      pipeTo(sender)
  }

  def rowFlow(flow: TMessages.RowFlow, sender: ActorRef): Unit = {
    val actorOpt = rowFlowLookup.get(flow.spec).orElse {
      Try {
        bundle.get.root.transform(RowTransformer(flow.spec.schema)).flatMap {
          rt =>
            flow.spec.options.select.map {
              s =>
                flow.spec.options.selectMode match {
                  case SelectMode.Strict => rt.select(s: _*)
                  case SelectMode.Relaxed => Try(rt.relaxedSelect(s: _*))
                }
            }.getOrElse(Try(rt))
        }
      }.flatMap(identity) match {
        case Success(rowTransformer) =>
          val actor = context.actorOf(RowFlowActor.props(rowTransformer, flow), s"row-stream-$streamId")
          context.watch(actor)

          streamId += 1
          rowFlowLookup += (flow.spec -> actor)
          rowFlowSpecLookup += (actor -> flow.spec)

          Some(actor)
        case Failure(err) =>
          sender ! Status.Failure(err)
          None
      }
    }

    for (actor <- actorOpt) {
      (actor ? RFMessages.GetRowTransformer) (flow.config.initTimeout).
        mapTo[(RowTransformer, Future[Done])].
        map(rt => (actor, rt)).
        pipeTo(sender)
    }
  }

  def unload(unload: TMessages.Unload, sender: ActorRef): Unit = {
    sender ! Done
    context.stop(self)
  }

  def receiveTimeout(): Unit = {
    if (rowFlowLookup.isEmpty) { context.stop(self) }
  }

  def terminated(ref: ActorRef): Unit = {
    for (spec <- rowFlowSpecLookup.get(ref)) {
      rowFlowSpecLookup -= ref
      rowFlowLookup -= spec
    }
  }
}
