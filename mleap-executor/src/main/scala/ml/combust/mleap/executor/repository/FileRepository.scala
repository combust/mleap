package ml.combust.mleap.executor.repository

import java.io.File
import java.net.URI
import java.nio.file.Path

import com.typesafe.config.Config

import scala.concurrent.Future

class FileRepository extends Repository {
  override def downloadBundle(uri: URI): Future[Path] = Future.successful(new File(uri).toPath)
}

class FileRepositoryProvider  extends RepositoryProvider {
  override def create(config: Config): FileRepository = new FileRepository
}
