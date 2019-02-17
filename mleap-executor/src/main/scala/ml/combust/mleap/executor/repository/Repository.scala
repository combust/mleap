package ml.combust.mleap.executor.repository

import java.net.URI
import java.nio.file.Path

import akka.actor.ActorSystem
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.concurrent.duration.TimeUnit

object Repository {
  /** Create a repository from a configuration.
    *
    * @param config typesafe config
    * @param system actor system
    * @return repository
    */
  def fromConfig(config: Config)
                (implicit system: ActorSystem): Repository = {
    val c = Class.forName(config.getString("class"))
    c.getField("MODULE$").get(c).
      asInstanceOf[RepositoryProvider].
      create(config)
  }
}

/** Repository is source for MLeap bundles that will
  *   ultimately be used for transforming data.
  */
trait Repository {
  /** Download the bundle specified by the URI
    * to a local file.
    *
    * @param uri uri of the bundle to download
    * @return future of the local file path after completion
    */
  def downloadBundle(uri: URI): Future[Path]

  /** Whether this repository can handle the given URI.
    *
    * @param uri uri for the bundle
    * @return true if it can handle, false otherwise
    */
  def canHandle(uri: URI): Boolean

  /** Close any resources this repository has.
    *
    */
  def shutdown(): Unit

  /** Await termination of this repository.
    *
    * @param timeout timeout
    * @param unit unit of timeout
    */
  def awaitTermination(timeout: Long, unit: TimeUnit): Unit
}

/** Can create repositories from Typesafe configs.
  */
trait RepositoryProvider {
  /** Crates a repository from a typesafe config.
    *
    * @param config repository configuration
    * @param system actor system
    * @return repository
    */
  def create(config: Config)(implicit system: ActorSystem): Repository
}
