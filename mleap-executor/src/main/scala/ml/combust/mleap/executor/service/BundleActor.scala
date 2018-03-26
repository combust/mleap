package ml.combust.mleap.executor.service

import java.net.URI

import akka.pattern.pipe
import akka.actor.{Actor, ActorRef, Props, ReceiveTimeout, Status}
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.executor._
import ml.combust.mleap.executor.service.BundleActor.{BundleLoaded, GetBundleMeta, RequestWithSender}
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
  case object Shutdown
}

class BundleActor(manager: TransformService,
                  uri: URI,
                  eventualBundle: Future[Bundle[Transformer]]) extends Actor {
  import context.dispatcher

  private val buffer: mutable.Queue[RequestWithSender] = mutable.Queue()
  private var bundle: Option[Bundle[Transformer]] = None
  private val rowTransformers: mutable.Map[StreamRowSpec, Try[RowTransformer]] = mutable.Map()

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
    case request: TransformRowRequest => maybeHandleRequestWithSender(RequestWithSender(request, sender()))
    case GetBundleMeta => maybeHandleRequestWithSender(RequestWithSender(GetBundleMeta, sender()))
    case bl: BundleActor.BundleLoaded => bundleLoaded(bl)
    case BundleActor.Shutdown => context.stop(self)
    case ReceiveTimeout => manager.unload(uri)
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
    case trr: TransformRowRequest => transformRow(trr, r.sender)
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

  def transformRow(request: TransformRowRequest, sender: ActorRef): Unit = {
    Future {
      rowTransformers.getOrElseUpdate(request.spec, createRowTransformer(request.spec)).map {
        rt => rt.transformOption(request.row)
      }
    }.flatMap(Future.fromTry).pipeTo(sender)
  }

  def createRowTransformer(spec: StreamRowSpec): Try[RowTransformer] = {
    bundle.get.root.transform(RowTransformer(spec.schema)).flatMap {
      rt => spec.options.select.map {
        s =>
          spec.options.selectMode match {
            case SelectMode.Strict => rt.select(s: _*)
            case SelectMode.Relaxed => Try(rt.relaxedSelect(s: _*))
          }
      }.getOrElse(Try(rt))
    }
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
