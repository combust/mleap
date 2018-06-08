package ml.combust.mleap.executor.service

import akka.Done
import akka.pattern.{ask, pipe}
import akka.actor.{Actor, ActorRef, Props, ReceiveTimeout, Status, Terminated}
import akka.stream.Materializer
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.executor._
import ml.combust.mleap.executor.repository.RepositoryBundleLoader
import ml.combust.mleap.runtime.frame.{RowTransformer, Transformer}

import scala.concurrent.duration._
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

case class RequestWithSender(request: Any, sender: ActorRef)

object BundleActor {
  def props(request: LoadModelRequest,
            loader: RepositoryBundleLoader)
           (implicit materializer: Materializer): Props = {
    Props(new BundleActor(request, loader))
  }

  object Messages {
    case class Loaded(bundle: Try[Bundle[Transformer]])
  }
}

class BundleActor(request: LoadModelRequest,
                  loader: RepositoryBundleLoader)
                 (implicit materializer: Materializer) extends Actor {
  import BundleActor.Messages
  import LocalTransformServiceActor.{Messages => TMessages}
  import RowStreamActor.{Messages => RFMessages}
  import FrameStreamActor.{Messages => FFMessages}
  import context.dispatcher

  private val model: Model = Model(request.modelName,
    request.uri,
    request.config)

  context.setReceiveTimeout(model.config.memoryTimeout)

  private val buffer: mutable.Queue[RequestWithSender] = mutable.Queue()
  private var bundle: Option[Bundle[Transformer]] = None

  private var rowStreamLookup: Map[String, ActorRef] = Map()
  private var rowStreamNameLookup: Map[ActorRef, String] = Map()

  private var frameStreamLookup: Map[String, ActorRef] = Map()
  private var frameStreamNameLookup: Map[ActorRef, String] = Map()

  private var loading: Boolean = false

  override def postStop(): Unit = {
    for (child <- context.children) {
      context.unwatch(child)
      context.stop(child)
    }
  }

  override def receive: Receive = {
    case request: GetBundleMetaRequest => maybeHandleRequest(RequestWithSender(request, sender))
    case request: CreateFrameStreamRequest => maybeHandleRequest(RequestWithSender(request, sender))
    case request: CreateRowStreamRequest => maybeHandleRequest(RequestWithSender(request, sender))
    case request: TransformFrameRequest => maybeHandleRequest(RequestWithSender(request, sender))
    case request: CreateFrameFlowRequest => maybeHandleRequest(RequestWithSender(request, sender))
    case request: CreateRowFlowRequest => maybeHandleRequest(RequestWithSender(request, sender))
    case request: UnloadModelRequest => unloadModel(request, sender)

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
    case r: GetBundleMetaRequest => getBundleMeta(r, request.sender)
    case r: TransformFrameRequest => transform(r, request.sender)
    case r: CreateFrameStreamRequest => createFrameStream(r, request.sender)
    case r: CreateRowStreamRequest => createRowStream(r, request.sender)
    case r: CreateFrameFlowRequest => createFrameFlow(r, request.sender)
    case r: CreateRowFlowRequest => createRowFlow(r, request.sender)
  }

  def maybeLoad(): Unit = {
    if (!loading) {
      loader.loadBundle(model.uri).map(Try(_)).recover {
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

  def getBundleMeta(request: GetBundleMetaRequest, sender: ActorRef): Unit = {
    for (bundle <- this.bundle) {
      sender ! BundleMeta(bundle.info, bundle.root.inputSchema, bundle.root.outputSchema)
    }
  }

  def transform(request: TransformFrameRequest, sender: ActorRef): Unit = {
    for (bundle <- this.bundle;
         transformer = bundle.root;
         frame = ExecuteTransform(transformer, request.frame, request.options)) {
      frame.flatMap(Future.fromTry).pipeTo(sender)
    }
  }

  def createFrameStream(request: CreateFrameStreamRequest, ref: ActorRef): Unit = {
    Try(context.actorOf(FrameStreamActor.props(bundle.get.root, request), s"frame/${request.streamName}")) match {
      case Success(actor) =>
        context.watch(actor)
        frameStreamLookup += (request.streamName -> actor)
        frameStreamNameLookup += (actor -> request.streamName)
        actor.tell(FFMessages.Initialize, sender)
      case Failure(err) => sender ! Status.Failure(err)
    }
  }

  def createRowStream(request: CreateRowStreamRequest, ref: ActorRef): Unit = {
    Try(context.actorOf(RowStreamActor.props(bundle.get.root, request), s"frame/${request.streamName}")) match {
      case Success(actor) =>
        context.watch(actor)
        frameStreamLookup += (request.streamName -> actor)
        frameStreamNameLookup += (actor -> request.streamName)
        actor.tell(RFMessages.Initialize, sender)
      case Failure(err) => sender ! Status.Failure(err)
    }
  }


  def createFrameFlow(request: CreateFrameFlowRequest, sender: ActorRef): Unit = {
    frameStreamLookup.get(request.streamName) match {
      case Some(actor) => actor.tell(request, sender)
      case None => sender ! Status.Failure(new NoSuchElementException(s"could not find stream ${request.modelName}/frame/${request.streamName}"))
    }
  }

  def createRowFlow(request: CreateRowFlowRequest, sender: ActorRef): Unit = {
    rowStreamLookup.get(request.streamName) match {
      case Some(actor) => actor.tell(request, sender)
      case None => sender ! Status.Failure(new NoSuchElementException(s"could not find stream ${request.modelName}/row/${request.streamName}"))
    }
  }

  def unloadModel(request: UnloadModelRequest, sender: ActorRef): Unit = {
    sender ! Done
    context.stop(self)
  }

  def receiveTimeout(): Unit = {
    if (rowStreamLookup.isEmpty) { context.stop(self) }
  }

  def terminated(ref: ActorRef): Unit = {
    for (name <- rowStreamNameLookup.get(ref)) {
      rowStreamLookup -= name
      rowStreamNameLookup -= ref
    }

    for (name <- frameStreamNameLookup.get(ref)) {
      frameStreamLookup -= name
      frameStreamNameLookup -= ref
    }
  }
}
