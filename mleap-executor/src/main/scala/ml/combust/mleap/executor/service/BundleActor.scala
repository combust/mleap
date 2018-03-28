package ml.combust.mleap.executor.service

import java.net.URI
import java.util.UUID

import akka.pattern.pipe
import akka.actor.{Actor, ActorRef, Props, ReceiveTimeout, Status}
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.executor._
import ml.combust.mleap.executor.service.BundleActor.{BundleLoaded, CreateRowTransformer, GetBundleMeta, RequestWithSender}
import ml.combust.mleap.runtime.frame.{RowTransformer, Transformer}

import scala.concurrent.duration._
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object BundleActor {
  def props(manager: TransformService,
            uri: URI,
            eventualBundle: Future[Bundle[Transformer]]): Props = {
    Props(new BundleActor(manager, uri, eventualBundle))
  }

  case object GetBundleMeta
  case class BundleLoaded(bundle: Try[Bundle[Transformer]])
  case class RequestWithSender(request: Any, sender: ActorRef)
  case class CreateRowTransformer(id: UUID, spec: StreamRowSpec)
  case class CloseStream(id: UUID)
  case object Shutdown
}

class BundleActor(manager: TransformService,
                  uri: URI,
                  eventualBundle: Future[Bundle[Transformer]]) extends Actor {
  import context.dispatcher

  private val buffer: mutable.Queue[RequestWithSender] = mutable.Queue()
  private var bundle: Option[Bundle[Transformer]] = None
  private val rowTransformers: mutable.Map[UUID, Try[RowTransformer]] = mutable.Map()

  // Probably want to make this timeout configurable eventually
  context.setReceiveTimeout(15.minutes)

  override def preStart(): Unit = {
    eventualBundle.onComplete {
      bundle => self ! BundleLoaded(bundle)
    }
  }

  override def postStop(): Unit = {
    eventualBundle.foreach(_.root.close())
  }

  override def receive: Receive = {
    case request: TransformFrameRequest => maybeHandleRequestWithSender(RequestWithSender(request, sender()))
    case request: BundleActor.CreateRowTransformer => maybeHandleRequestWithSender(RequestWithSender(request, sender()))
    case GetBundleMeta => maybeHandleRequestWithSender(RequestWithSender(GetBundleMeta, sender()))
    case bl: BundleActor.BundleLoaded => bundleLoaded(bl)
    case BundleActor.Shutdown => context.stop(self)
    case BundleActor.CloseStream(id) => rowTransformers -= id
    case ReceiveTimeout => handleTimeout()
  }

  def maybeHandleRequestWithSender(r: RequestWithSender): Unit = {
    if (bundle.isEmpty) {
      buffer.enqueue(r)
    } else {
      handleRequestWithSender(r)
    }
  }

  def handleRequestWithSender(r: BundleActor.RequestWithSender): Unit = r.request match {
    case tfr: TransformFrameRequest => transformFrame(tfr, r.sender)
    case crt: CreateRowTransformer => createRowTransformer(crt, r.sender)
    case GetBundleMeta => handleGetBundleMeta(r.sender)
  }

  def handleGetBundleMeta(sender: ActorRef): Unit = {
    for(bundle <- this.bundle) {
      sender ! BundleMeta(bundle.info, bundle.root.inputSchema, bundle.root.outputSchema)
    }
  }

  def transformFrame(request: TransformFrameRequest, sender: ActorRef): Unit = {
    for(bundle <- this.bundle;
        transformer = bundle.root;
        frame = ExecuteTransform(transformer, request)) {
      frame.pipeTo(sender)
    }
  }

  def handleTimeout(): Unit = {
    // Only unload on timeout if there are no open streams
    if (rowTransformers.isEmpty) { manager.unload(uri) }
  }

  def createRowTransformer(request: CreateRowTransformer, sender: ActorRef): Unit = {
    Future.fromTry {
      bundle.get.root.transform(RowTransformer(request.spec.schema)).flatMap {
        rt => request.spec.options.select.map {
          s =>
            request.spec.options.selectMode match {
              case SelectMode.Strict => rt.select(s: _*)
              case SelectMode.Relaxed => Try(rt.relaxedSelect(s: _*))
            }
        }.getOrElse(Try(rt))
      }
    }.pipeTo(sender)
  }

  def bundleLoaded(loaded: BundleActor.BundleLoaded): Unit = {
    loaded.bundle match {
      case Success(b) =>
        this.bundle = Some(b)
        for(r <- this.buffer.dequeueAll(_ => true)) {
          handleRequestWithSender(r)
        }
      case Failure(error) =>
        for(r <- this.buffer.dequeueAll(_ => true)) {
          r.sender ! Status.Failure(error)
        }
        context.stop(self)
    }
  }
}
