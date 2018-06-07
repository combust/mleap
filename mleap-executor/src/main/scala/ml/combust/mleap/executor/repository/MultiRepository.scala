package ml.combust.mleap.executor.repository

import java.net.URI
import java.nio.file.Path
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.util.Try

class MultiRepository(repositories: Seq[Repository]) extends Repository {
  val terminatePromise: Promise[Unit] = Promise[Unit]
  new Thread {
    override def run(): Unit = {
      terminatePromise.complete(Try {
        for (repository <- repositories) { repository.awaitTermination(Long.MaxValue, TimeUnit.DAYS) }
      })
    }
  }.start()

  override def downloadBundle(uri: URI): Future[Path] = {
    for (repository <- repositories) {
      if (repository.canHandle(uri)) return repository.downloadBundle(uri)
    }

    Future.failed(new BundleException("could not find a repository to download the bundle file"))
  }

  override def canHandle(uri: URI): Boolean = {
    for (repository <- repositories) {
      if (repository.canHandle(uri)) return true
    }

    false
  }

  override def shutdown(): Unit = {
    for (repository <- repositories) { repository.shutdown() }
  }

  override def awaitTermination(timeout: Long, unit: TimeUnit): Unit = synchronized {
    Try(Await.ready(terminatePromise.future, FiniteDuration(timeout, unit)))
  }
}

object MultiRepositoryProvider extends RepositoryProvider {
  override def create(config: Config)
                     (implicit system: ActorSystem): MultiRepository = {
    val rConfigs = config.getConfigList("repositories")

    val repositories = for (rConfig <- rConfigs.asScala) yield {
      Repository.fromConfig(rConfig)
    }

    new MultiRepository(repositories)
  }
}
