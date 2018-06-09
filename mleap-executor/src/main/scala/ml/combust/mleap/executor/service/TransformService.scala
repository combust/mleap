package ml.combust.mleap.executor.service

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.javadsl
import akka.stream.scaladsl.Flow
import ml.combust.mleap.executor._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

trait TransformService {
  def close(): Unit

  def getBundleMeta(request: GetBundleMetaRequest)
                   (implicit timeout: FiniteDuration): Future[BundleMeta]

  def getBundleMeta(request: GetBundleMetaRequest, timeout: Int): Future[BundleMeta] = {
    getBundleMeta(request)(FiniteDuration(timeout, TimeUnit.MILLISECONDS))
  }

  def loadModel(request: LoadModelRequest)
               (implicit timeout: FiniteDuration): Future[Model]

  def unloadModel(request: UnloadModelRequest)
                 (implicit timeout: FiniteDuration): Future[Model]

  def createFrameStream(request: CreateFrameStreamRequest)
                       (implicit timeout: FiniteDuration): Future[FrameStream]

  def createRowStream(request: CreateRowStreamRequest)
                       (implicit timeout: FiniteDuration): Future[RowStream]

  def transform(request: TransformFrameRequest)
               (implicit timeout: FiniteDuration): Future[Try[DefaultLeapFrame]]

  def transform(request: TransformFrameRequest,
                timeout: Int): Future[Try[DefaultLeapFrame]] = {
    transform(request)(FiniteDuration(timeout, TimeUnit.MILLISECONDS))
  }

  def frameFlow[Tag: TagBytes](request: CreateFrameFlowRequest)
                    (implicit timeout: FiniteDuration): Flow[(StreamTransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed]

  def rowFlow[Tag: TagBytes](request: CreateRowFlowRequest)
                  (implicit timeout: FiniteDuration): Flow[(StreamTransformRowRequest, Tag), (Try[Option[Row]], Tag), NotUsed]

  def javaRowFlow[Tag: TagBytes](request: CreateRowFlowRequest)
                      (implicit timeout: FiniteDuration): javadsl.Flow[(StreamTransformRowRequest, Tag), (Try[Option[Row]], Tag), NotUsed] = {
    rowFlow(request).asJava
  }
}
