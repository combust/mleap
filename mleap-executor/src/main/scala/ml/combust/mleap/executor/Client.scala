package ml.combust.mleap.executor

import java.net.URI

import akka.NotUsed
import akka.stream.scaladsl.Flow
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

import scala.concurrent.Future
import scala.util.Try

trait RowTransformClient extends AutoCloseable {
  def transform(row: Row): Future[Option[Row]]
  def flow[Tag]: Flow[(TransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed]
}

trait Client {
  def getBundleMeta(uri: URI): Future[BundleMeta]
  def transform(uri: URI, request: TransformFrameRequest): Future[DefaultLeapFrame]
  def rowTransformClient(uri: URI, spec: StreamRowSpec): Future[RowTransformClient]
  def rowFlow[Tag](uri: URI, spec: StreamRowSpec): Flow[(TransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed]
}
