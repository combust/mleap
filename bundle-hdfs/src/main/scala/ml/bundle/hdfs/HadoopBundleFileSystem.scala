package ml.bundle.hdfs

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

import com.typesafe.config.Config
import ml.combust.bundle.fs.BundleFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Try
import scala.collection.JavaConverters._

object HadoopBundleFileSystem {
  lazy val defaultSchemes: Seq[String] = Seq("hdfs")

  def createHadoopConfiguration(config: Config): Configuration = {
    val options: Map[String, String] = if(config.hasPath("options")) {
      config.getConfig("options").entrySet().asScala.map {
        entry => (entry.getKey, entry.getValue.unwrapped().toString)
      }.toMap
    } else {
      Map()
    }

    val c = new Configuration()
    for ((key, value) <- options) { c.set(key, value) }
    c
  }

  def createSchemes(config: Config): Seq[String] = if (config.hasPath("schemes")) {
    config.getStringList("schemes").asScala.toSeq
  } else { Seq("hdfs") }
}

class HadoopBundleFileSystem(fs: FileSystem,
                             override val schemes: Seq[String] = HadoopBundleFileSystem.defaultSchemes) extends BundleFileSystem {
  def this(config: Config) = {
    this(FileSystem.get(HadoopBundleFileSystem.createHadoopConfiguration(config)),
      HadoopBundleFileSystem.createSchemes(config))
  }

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
