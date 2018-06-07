package ml.combust.mleap.executor

import java.net.URI
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.typesafe.config.ConfigFactory
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

trait TagBytes[Tag] {
  def toBytes(t: Tag): Array[Byte]
  def fromBytes(bytes: Array[Byte]): Tag
}

object TagBytes {
  implicit object UUIDTagBytes$ extends TagBytes[UUID] {
    override def toBytes(t: UUID): Array[Byte] = {
      val bb = ByteBuffer.wrap(new Array[Byte](16))
      bb.putLong(t.getMostSignificantBits)
      bb.putLong(t.getLeastSignificantBits)
      bb.array()
    }

    override def fromBytes(bytes: Array[Byte]): UUID = {
      val bb = ByteBuffer.wrap(bytes)
      new UUID(bb.getLong, bb.getLong)
    }
  }
}

trait RowTransformClient extends AutoCloseable {
  def transform(row: Row): Future[Option[Row]]
}

object Client {
  private val config = ConfigFactory.load().getConfig("ml.combust.mleap.executor.client")

  val defaultTimeout: FiniteDuration = FiniteDuration(config.getDuration("default-timeout").toMillis, TimeUnit.MILLISECONDS)
  val defaultParallelism: Parallelism = Parallelism(config.getInt("default-parallelism"))
}

trait Client {
  def getBundleMeta(uri: URI): Future[BundleMeta]

  def transform(uri: URI, request: TransformFrameRequest)
               (implicit timeout: FiniteDuration = Client.defaultTimeout): Future[DefaultLeapFrame]

  def rowTransformClient(uri: URI, spec: StreamRowSpec): RowTransformClient = ???

  def frameFlow[Tag: TagBytes](uri: URI, options: TransformOptions = TransformOptions.default)
                              (implicit timeout: FiniteDuration = Client.defaultTimeout,
                               parallelism: Parallelism = Client.defaultParallelism): Flow[(TransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed]

  def rowFlow[Tag: TagBytes](uri: URI, spec: StreamRowSpec)
                            (implicit timeout: FiniteDuration = Client.defaultTimeout,
                             parallelism: Parallelism = Client.defaultParallelism): Flow[(Try[Row], Tag), (Try[Option[Row]], Tag), NotUsed]
}
