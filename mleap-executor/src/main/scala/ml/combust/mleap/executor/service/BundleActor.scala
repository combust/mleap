package ml.combust.mleap.executor.service

import akka.pattern.pipe
import akka.actor.{Actor, ActorRef, Props, ReceiveTimeout, Status, Terminated}
import akka.stream.Materializer
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.executor._
import ml.combust.mleap.executor.error.{AlreadyExistsException, ExecutorException, NotFoundException}
import ml.combust.mleap.executor.repository.RepositoryBundleLoader
import ml.combust.mleap.runtime.frame.Transformer

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

case class RequestWithSender(request: Any, sender: ActorRef)

object BundleActor {
  def props(request: LoadModelRequest,
            loader: RepositoryBundleLoader,
            config: ExecutorConfig)
           (implicit materializer: Materializer): Props = {
    Props(new BundleActor(request, loader, config))
  }

  object Messages {
    case class Loaded(bundle: Try[Bundle[Transformer]])
  }
}

class BundleActor(request: LoadModelRequest,
                  loader: RepositoryBundleLoader,
                  config: ExecutorConfig)
                 (implicit materializer: Materializer) extends Actor {
  import BundleActor.Messages
  import RowStreamActor.{Messages => RFMessages}
  import FrameStreamActor.{Messages => FFMessages}
  import context.dispatcher

  private val model: Model = Model(request.modelName,
    request.uri,
    request.config)

  context.setReceiveTimeout(model.config.memoryTimeout.getOrElse(config.defaultMemoryTimeout))

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
    case request: TransformFrameRequest => maybeHandleRequest(RequestWithSender(request, sender))
    case request: GetBundleMetaRequest => maybeHandleRequest(RequestWithSender(request, sender))
    case request: CreateFrameStreamRequest => maybeHandleRequest(RequestWithSender(request, sender))
    case request: CreateRowStreamRequest => maybeHandleRequest(RequestWithSender(request, sender))
    case request: GetRowStreamRequest => maybeHandleRequest(RequestWithSender(request, sender))
    case request: GetFrameStreamRequest => maybeHandleRequest(RequestWithSender(request, sender))
    case request: CreateFrameFlowRequest => maybeHandleRequest(RequestWithSender(request, sender))
    case request: CreateRowFlowRequest => maybeHandleRequest(RequestWithSender(request, sender))
    case request: UnloadModelRequest => unloadModel(request, sender)
    case request: LoadModelRequest => loadModel(request, sender)

    case request: Messages.Loaded => loaded(request)
    case request: GetModelRequest => getModel(request)

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
    case r: TransformFrameRequest => transform(r, request.sender)
    case r: GetBundleMetaRequest => getBundleMeta(r, request.sender)
    case r: CreateFrameStreamRequest => createFrameStream(r, request.sender)
    case r: CreateRowStreamRequest => createRowStream(r, request.sender)
    case r: GetRowStreamRequest => getRowStream(r, request.sender)
    case r: GetFrameStreamRequest => getFrameStream(r, request.sender)
    case r: CreateFrameFlowRequest => createFrameFlow(r, request.sender)
    case r: CreateRowFlowRequest => createRowFlow(r, request.sender)
    case _ => request.sender ! Status.Failure(new ExecutorException(s"unknown request $request"))
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

  def getModel(request: GetModelRequest): Unit = {
    sender ! model
  }

  def transform(request: TransformFrameRequest, sender: ActorRef): Unit = {
    for (bundle <- this.bundle;
         transformer = bundle.root;
         frame = ExecuteTransform(transformer, request.frame, request.options)) yield {
      frame.pipeTo(sender)
    }
  }

  def createFrameStream(request: CreateFrameStreamRequest, sender: ActorRef): Unit = {
    context.child(request.streamName) match {
      case Some(_) =>
        sender ! Status.Failure(new AlreadyExistsException(s"stream already exists ${request.modelName}/${request.streamName}"))
      case None =>
        Try(context.actorOf(FrameStreamActor.props(bundle.get.root, request), request.streamName)) match {
          case Success(actor) =>
            context.watch(actor)
            frameStreamLookup += (request.streamName -> actor)
            frameStreamNameLookup += (actor -> request.streamName)
            actor.tell(FFMessages.Initialize, sender)
          case Failure(err) => sender ! Status.Failure(err)
        }
    }
  }

  def createRowStream(request: CreateRowStreamRequest, sender: ActorRef): Unit = {
    context.child(request.streamName) match {
      case Some(_) =>
        sender ! Status.Failure(new AlreadyExistsException(s"stream already exists ${request.modelName}/${request.streamName}"))
      case None =>
        Try(context.actorOf(RowStreamActor.props(bundle.get.root, request), request.streamName)) match {
          case Success(actor) =>
            context.watch(actor)
            rowStreamLookup += (request.streamName -> actor)
            rowStreamNameLookup += (actor -> request.streamName)
            actor.tell(RFMessages.Initialize, sender)
          case Failure(err) =>
            err.printStackTrace()
            sender ! Status.Failure(err)
        }
    }
  }

  def getRowStream(request: GetRowStreamRequest, sender: ActorRef): Unit = {
    rowStreamLookup.get(request.streamName) match {
      case Some(actor) => actor.tell(request, sender)
      case None => sender ! Status.Failure(new NotFoundException(s"could not find stream ${request.modelName}/${request.streamName}"))
    }
  }

  def getFrameStream(request: GetFrameStreamRequest, sender: ActorRef): Unit = {
    frameStreamLookup.get(request.streamName) match {
      case Some(actor) => actor.tell(request, sender)
      case None => sender ! Status.Failure(new NotFoundException(s"could not find stream ${request.modelName}/${request.streamName}"))
    }
  }

  def createFrameFlow(request: CreateFrameFlowRequest, sender: ActorRef): Unit = {
    frameStreamLookup.get(request.streamName) match {
      case Some(actor) => actor.tell(request, sender)
      case None => sender ! Status.Failure(new NotFoundException(s"could not find stream ${request.modelName}/${request.streamName}"))
    }
  }

  def createRowFlow(request: CreateRowFlowRequest, sender: ActorRef): Unit = {
    rowStreamLookup.get(request.streamName) match {
      case Some(actor) => actor.tell(request, sender)
      case None => sender ! Status.Failure(new NotFoundException(s"could not find stream ${request.modelName}/${request.streamName}"))
    }
  }

  def unloadModel(request: UnloadModelRequest, sender: ActorRef): Unit = {
    sender ! model
    context.stop(self)
  }

  def loadModel(request: LoadModelRequest, sender: ActorRef): Unit = {
    maybeLoad()
    sender ! model
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
