package ml.combust.mleap.executor.repository

import java.net.URI
import java.nio.file.{Files, Path}

import akka.actor.ActorSystem
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.concurrent.duration.TimeUnit

class HttpRepository extends Repository {
  override def downloadBundle(uri: URI): Future[Path] = Future {
    val tmpFile = Files.createTempFile("mleap", ".bundle.zip")
    Files.copy(uri.toURL.openStream(), tmpFile)
    tmpFile
  }

  override def canHandle(uri: URI): Boolean = uri.getScheme == "http" || uri.getScheme == "https"

  override def shutdown(): Unit = { }

  override def awaitTermination(timeout: Long, unit: TimeUnit): Unit = { }
}

object HttpRepositoryProvider extends RepositoryProvider {
  override def create(config: Config)
                     (implicit system: ActorSystem): HttpRepository = new HttpRepository
}
