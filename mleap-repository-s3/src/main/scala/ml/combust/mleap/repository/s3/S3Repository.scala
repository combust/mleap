package ml.combust.mleap.repository.s3

import java.net.URI
import java.nio.file.{Files, Path}
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import com.amazonaws.services.s3.{AmazonS3ClientBuilder, AmazonS3URI}
import com.typesafe.config.Config
import ml.combust.mleap.executor.repository.{Repository, RepositoryProvider}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.TimeUnit
import scala.util.Try

class S3RepositoryConfig(config: Config) {
  val threads: Int = config.getInt("threads")
}

class S3Repository(config: S3RepositoryConfig) extends Repository {
  private val client = AmazonS3ClientBuilder.defaultClient()
  private val threadPool = Executors.newFixedThreadPool(config.threads)
  implicit val diskEc: ExecutionContext = ExecutionContext.fromExecutor(threadPool)

  override def downloadBundle(uri: URI): Future[Path] = Future {
    val s3Uri = new AmazonS3URI(uri)
    val bucket = s3Uri.getBucket
    val key = s3Uri.getKey

    val tmpFile = Files.createTempFile("mleap", ".bundle.zip")
    Files.copy(client.getObject(bucket, key).getObjectContent, tmpFile)
    tmpFile
  }

  override def canHandle(uri: URI): Boolean = Try(new AmazonS3URI(uri)).isSuccess

  override def shutdown(): Unit = threadPool.shutdown()
  override def awaitTermination(timeout: Long, unit: TimeUnit): Unit = threadPool.awaitTermination(timeout, unit)
}

class S3RepositoryProvider extends RepositoryProvider {
  override def create(config: Config)
                     (implicit system: ActorSystem): S3Repository = {
    new S3Repository(new S3RepositoryConfig(config))
  }
}
