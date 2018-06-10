package ml.combust.mleap.executor.service

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.javadsl
import akka.stream.scaladsl.Flow
import ml.combust.mleap.executor._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

import scala.concurrent.duration._
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

  def getModel(request: GetModelRequest)
              (implicit timeout: FiniteDuration): Future[Model]

  def loadModel(request: LoadModelRequest)
               (implicit timeout: FiniteDuration): Future[Model]

  def unloadModel(request: UnloadModelRequest)
                 (implicit timeout: FiniteDuration): Future[Model]

  def createFrameStream(request: CreateFrameStreamRequest)
                       (implicit timeout: FiniteDuration): Future[FrameStream]

  def getFrameStream(request: GetFrameStreamRequest)
                    (implicit timeout: FiniteDuration): Future[FrameStream]

  def createRowStream(request: CreateRowStreamRequest)
                     (implicit timeout: FiniteDuration): Future[RowStream]

  def getRowStream(request: GetRowStreamRequest)
                  (implicit timeout: FiniteDuration): Future[RowStream]

  def transform(request: TransformFrameRequest)
               (implicit timeout: FiniteDuration): Future[Try[DefaultLeapFrame]]

  def transform(request: TransformFrameRequest,
                timeout: Int): Future[Try[DefaultLeapFrame]] = {
    transform(request)(FiniteDuration(timeout, TimeUnit.MILLISECONDS))
  }

  def createFrameFlow[Tag](request: CreateFrameFlowRequest)
                          (implicit timeout: FiniteDuration): Flow[(StreamTransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed]

  def createRowFlow[Tag](request: CreateRowFlowRequest)
                        (implicit timeout: FiniteDuration): Flow[(StreamTransformRowRequest, Tag), (Try[Option[Row]], Tag), NotUsed]

  def javaRowFlow[Tag](request: CreateRowFlowRequest,
                       timeout: Long): javadsl.Flow[(StreamTransformRowRequest, Tag), (Try[Option[Row]], Tag), NotUsed] = {
    createRowFlow(request)(timeout.millis).asJava
  }
}
