package ml.combust.bundle.test

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

import ml.combust.bundle.fs.BundleFileSystem

import scala.util.Try

class TestBundleFileSystem extends BundleFileSystem {
  override def schemes: Seq[String] = Seq("test")

  override def load(uri: URI): Try[File] = {
    Try(new File(uri.getPath))
  }

  override def save(uri: URI, localFile: File): Unit = {
    Files.copy(localFile.toPath, Paths.get(uri.getPath))
  }
}
