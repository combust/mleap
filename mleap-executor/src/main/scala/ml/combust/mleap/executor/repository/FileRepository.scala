package ml.combust.mleap.executor.repository

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path, StandardCopyOption}
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.TimeUnit
import scala.concurrent.{ExecutionContext, Future}

object FileRepositoryConfig {
  val defaults: Config = ConfigFactory.load().getConfig("ml.combust.mleap.executor.repository-defaults.file")
}

class FileRepositoryConfig(_config: Config) {
  val config: Config = _config.withFallback(FileRepositoryConfig.defaults)

  val move: Boolean = config.getBoolean("move")
  val threads: Int = config.getInt("threads")
}

class FileRepository(config: FileRepositoryConfig) extends Repository {
  private val threadPool = Executors.newFixedThreadPool(config.threads)
  implicit val diskEc: ExecutionContext = ExecutionContext.fromExecutor(threadPool)

  def this() = this(new FileRepositoryConfig(FileRepositoryConfig.defaults))

  override def downloadBundle(uri: URI): Future[Path] = Future {

    if (uri.getPath.isEmpty) {
      throw new BundleException("file path cannot be empty")
    }

    val local = new File(uri.getPath).toPath
    if (!Files.exists(local)) {
      throw new BundleException(s"file does not exist $local")
    }

    if (config.move) {
      val tmpFile = Files.createTempFile("mleap", ".bundle.zip")
      Files.copy(local, tmpFile, StandardCopyOption.REPLACE_EXISTING)
      tmpFile.toFile.deleteOnExit()
      tmpFile
    } else {
      local
    }
  }

  override def canHandle(uri: URI): Boolean = uri.getScheme == "file" || uri.getScheme == "jar:file"

  override def shutdown(): Unit = threadPool.shutdown()

  override def awaitTermination(timeout: Long, unit: TimeUnit): Unit = threadPool.awaitTermination(timeout, unit)
}

object FileRepositoryProvider extends RepositoryProvider {
  override def create(tConfig: Config)
                     (implicit system: ActorSystem): Repository = {
    val config = new FileRepositoryConfig(tConfig)

    new FileRepository(config)
  }
}
