package ml.combust.bundle.fs


import java.io.File
import java.nio.file.Files

import scala.util.Try

class LocalFileSystemBundleFileSystem() extends BundleFileSystem {
  override def load(path: String): Try[File] = Try {
    val tmp = Files.createTempFile("bunlde", ".zip")
    Files.copy(new File(path).toPath, tmp)
    tmp.toFile
  }

  override def save(path: String, localFile: File): Unit = {
    Files.copy(localFile.toPath, new File(path).toPath)
  }
}
