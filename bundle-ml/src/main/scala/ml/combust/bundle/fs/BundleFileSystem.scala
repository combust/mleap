package ml.combust.bundle.fs

import java.io.File

import scala.util.Try

trait BundleFileSystem {
  def load(path: String): Try[File]
  def save(path: String, localFile: File): Unit
}
