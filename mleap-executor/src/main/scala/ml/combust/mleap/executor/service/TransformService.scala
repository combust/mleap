package ml.combust.mleap.executor.service

import java.net.URI
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.javadsl
import akka.stream.scaladsl.Flow
import ml.combust.mleap.executor.{BundleMeta, Parallelism, StreamRowSpec, TransformFrameRequest}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, RowTransformer}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

trait TransformService {
  def close(): Unit

  def getBundleMeta(uri: URI)
                   (implicit timeout: FiniteDuration): Future[BundleMeta]

  def getBundleMeta(uri: URI, timeout: Int): Future[BundleMeta] = {
    getBundleMeta(uri)(FiniteDuration(timeout, TimeUnit.MILLISECONDS))
  }

  def transform(uri: URI, request: TransformFrameRequest)
               (implicit timeout: FiniteDuration): Future[DefaultLeapFrame]

  def transform(uri: URI,
                request: TransformFrameRequest,
                timeout: Int): Future[DefaultLeapFrame] = {
    transform(uri, request)(FiniteDuration(timeout, TimeUnit.MILLISECONDS))
  }

  def frameFlow[Tag](uri: URI)
                    (implicit timeout: FiniteDuration,
                     parallelism: Parallelism): Flow[(TransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed]

  def rowFlow[Tag](uri: URI,
                   spec: StreamRowSpec)
                  (implicit timeout: FiniteDuration,
                   parallelism: Parallelism): Flow[(Try[Row], Tag), (Try[Option[Row]], Tag), Future[RowTransformer]]

  def javaRowFlow[Tag](uri: URI,
                       spec: StreamRowSpec,
                       timeout: Int,
                       parallelism: Int): javadsl.Flow[(Try[Row], Tag), (Try[Option[Row]], Tag), Future[RowTransformer]] = {
    rowFlow(uri, spec)(FiniteDuration(timeout, TimeUnit.MILLISECONDS), parallelism).asJava
  }

  def unload(uri: URI): Unit
}
