package ml.combust.bundle

import java.io.{Closeable, File}
import java.net.URI
import java.nio.file.{FileSystem, FileSystems, Files, Path}
import java.util.stream.Collectors

import ml.combust.bundle.dsl.{Bundle, BundleInfo}
import resource._
import ml.combust.bundle.json.JsonSupport._
import spray.json._

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Try
import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 12/24/16.
  */
object BundleFile {
  implicit def apply(uri: String): BundleFile = {
    apply(new URI(uri))
  }

  implicit def apply(file: File): BundleFile = {
    val uri = if(file.getPath.endsWith(".zip")) {
      new URI(s"jar:file:${file.getPath}")
    } else {
      new URI(s"file:$file")
    }

    apply(uri)
  }

  implicit def apply(uri: URI): BundleFile = {
    val env = Map("create" -> "true").asJava

    val (fs, path) = uri.getScheme match {
      case "file" =>
        (FileSystems.getDefault, FileSystems.getDefault.getPath(uri.getPath))
      case "jar" =>
        val zfs = FileSystems.newFileSystem(uri, env)
        (zfs, zfs.getPath("/"))
    }

    apply(fs, path)
  }
}

case class BundleFile(fs: FileSystem,
                      path: Path) extends Closeable {
  /** Read bundle definition from the path.
    *
    * @return bundle definition
    */
  def readInfo(): BundleInfo = {
    val bundleJson = fs.getPath(path.toString, Bundle.bundleJson)
    (for(in <- managed(Files.newInputStream(bundleJson))) yield {
      val json = Source.fromInputStream(in).getLines.mkString
      json.parseJson.convertTo[BundleInfo]
    }).either.either match {
      case Left(errors) => throw errors.head
      case Right(info) => info
    }
  }

  def writeNote(name: String, note: String): Unit = {
    Files.createDirectories(fs.getPath(path.toString, "notes"))
    for(out <- managed(Files.newOutputStream(fs.getPath(path.toString, "notes", name)))) {
      out.write(note.getBytes)
    }
  }

  def readNote(name: String): String = {
    new String(Files.readAllBytes(fs.getPath(path.toString, "notes", name)))
  }

  def listNotes(): Set[String] = {
    Files.list(fs.getPath(path.toString, "notes")).
      collect(Collectors.toList()).asScala.
      map(_.getFileName.toString).
      toSet
  }

  override def close(): Unit = {
    // closing some file systems, like Unix, raises an
    // unsupported operation error
    Try(fs.close())
  }
}
