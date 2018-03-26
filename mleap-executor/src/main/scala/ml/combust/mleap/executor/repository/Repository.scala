package ml.combust.mleap.executor.repository

import java.net.URI
import java.nio.file.Path

import com.typesafe.config.Config

import scala.concurrent.Future

object Repository {
  def create(config: Config): Repository = ???
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
}

/** Can create repositories from Typesafe configs.
  */
trait RepositoryProvider {
  /** Crates a repository from a typesafe config.
    *
    * @param config repository configuration
    * @return repository
    */
  def create(config: Config): Repository
}
