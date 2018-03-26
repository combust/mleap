package ml.combust.mleap.executor.repository

import java.net.URI
import java.nio.file.Path

import com.typesafe.config.Config

import scala.concurrent.Future

class MultiRepository extends Repository {
  override def downloadBundle(uri: URI): Future[Path] = ???
}

class MultiRepositoryProvider extends RepositoryProvider {
  override def create(config: Config): MultiRepository = ???
}
