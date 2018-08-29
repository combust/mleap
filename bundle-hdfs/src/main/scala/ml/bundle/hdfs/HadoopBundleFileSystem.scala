package ml.bundle.hdfs

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

import com.typesafe.config.Config
import ml.combust.bundle.fs.BundleFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Try

class HadoopBundleFileSystem(fs: FileSystem) extends BundleFileSystem {
  def this(config: Config) = {
    this(FileSystem.get(new Configuration()))
  }

  override def scheme: String = "hdfs"

  override def load(uri: URI): Try[File] = Try {
    val tmpDir = Files.createTempDirectory("hdfs-bundle")
    val tmpFile = Paths.get(tmpDir.toString, "bundle.zip")
    fs.copyToLocalFile(new Path(uri.toString), new Path(tmpFile.toString))
    tmpFile.toFile
  }

  override def save(uri: URI, localFile: File): Unit = {
    fs.copyFromLocalFile(new Path(localFile.toString), new Path(uri.toString))
  }
}
