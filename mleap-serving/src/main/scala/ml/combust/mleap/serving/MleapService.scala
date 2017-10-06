package ml.combust.mleap.serving

import java.io.File

import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}
import ml.combust.mleap.serving.domain.v1.{LoadModelRequest, LoadModelResponse, UnloadModelRequest, UnloadModelResponse}
import ml.combust.mleap.runtime.frame.MleapSupport._
import resource._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Created by hollinwilkins on 1/30/17.
  */
class MleapService()
                  (implicit ec: ExecutionContext) {
  private var bundle: Option[Bundle[Transformer]] = None

  def setBundle(bundle: Bundle[Transformer]): Unit = synchronized(this.bundle = Some(bundle))
  def unsetBundle(): Unit = synchronized(this.bundle = None)

  def loadModel(request: LoadModelRequest): Future[LoadModelResponse] = Future {
    (for(bf <- managed(BundleFile(new File(request.path.get.toString)))) yield {
      bf.loadMleapBundle()
    }).tried.flatMap(identity)
  }.flatMap(r => Future.fromTry(r)).andThen {
    case Success(b) => setBundle(b)
  }.map(_ => LoadModelResponse())

  def unloadModel(request: UnloadModelRequest): Future[UnloadModelResponse] = {
    unsetBundle()
    Future.successful(UnloadModelResponse())
  }

  def transform(frame: DefaultLeapFrame): Try[DefaultLeapFrame] = synchronized {
    bundle.map {
      _.root.transform(frame)
    }.getOrElse(Failure(new IllegalStateException("no transformer loaded")))
  }

  def getSchema: Try[StructType] = synchronized {
    bundle.map {
      bundle => Success(bundle.root.schema)
    }.getOrElse(Failure(new IllegalStateException("no transformer loaded")))
  }
}
