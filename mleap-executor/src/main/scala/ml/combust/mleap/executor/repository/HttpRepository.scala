package ml.combust.mleap.executor.repository

import java.net.URI
import java.nio.file.{Files, Path}
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.TimeUnit

object HttpRepositoryConfig {
  val defaults: Config = ConfigFactory.load().getConfig("ml.combust.mleap.executor.repository-defaults.http")
}

class HttpRepositoryConfig(_config: Config) {
  val config: Config = _config.withFallback(FileRepositoryConfig.defaults)

  val threads: Int = config.getInt("threads")
}

class HttpRepository(config: HttpRepositoryConfig) extends Repository {
  private val threadPool = Executors.newFixedThreadPool(config.threads)
  implicit val diskEc: ExecutionContext = ExecutionContext.fromExecutor(threadPool)

  override def downloadBundle(uri: URI): Future[Path] = Future {
    val tmpFile = Files.createTempFile("mleap", ".bundle.zip")
    Files.copy(uri.toURL.openStream(), tmpFile)
    tmpFile
  }

  override def canHandle(uri: URI): Boolean = uri.getScheme == "http" || uri.getScheme == "https"

  override def shutdown(): Unit = threadPool.shutdown()

  override def awaitTermination(timeout: Long, unit: TimeUnit): Unit = threadPool.awaitTermination(timeout, unit)
}

object HttpRepositoryProvider extends RepositoryProvider {
  override def create(config: Config)
                     (implicit system: ActorSystem): HttpRepository = {
    new HttpRepository(new HttpRepositoryConfig(config))
  }
}
