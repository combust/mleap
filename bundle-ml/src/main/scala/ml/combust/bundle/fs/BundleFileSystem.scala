package ml.combust.bundle.fs

import java.io.File
import java.net.URI

import scala.util.Try

trait BundleFileSystem {
  def schemes: Seq[String]
  def load(uri: URI): Try[File]
  def save(uri: URI, localFile: File): Unit
}
