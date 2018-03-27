package ml.combust.mleap.executor.repository

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path, StandardCopyOption}

import scala.concurrent.{ExecutionContext, Future}

class FileRepository(copy: Boolean)
                    (implicit diskEc: ExecutionContext) extends Repository {
  override def downloadBundle(uri: URI): Future[Path] = Future {
    val local = new File(uri.getPath).toPath
    if (!Files.exists(local)) {
      throw new IllegalArgumentException(s"file does not exist $local")
    }

    if (copy) {
      val tmpFile = Files.createTempFile("mleap", ".bundle.zip")
      Files.copy(local, tmpFile, StandardCopyOption.REPLACE_EXISTING)
      tmpFile.toFile.deleteOnExit()
      tmpFile
    } else {
      local
    }
  }
}

//class FileRepositoryProvider extends RepositoryProvider {
//  override def create(config: Config): FileRepository = new FileRepository(true)
//}
