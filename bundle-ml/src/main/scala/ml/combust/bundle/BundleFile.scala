package ml.combust.bundle

import java.io.{Closeable, File}
import java.net.URI
import java.nio.file.{FileSystem, FileSystems, Files, Path}
import java.util.stream.Collectors
import ml.combust.bundle.dsl.{Bundle, BundleInfo}
import ml.combust.bundle.fs.BundleFileSystem
import ml.combust.bundle.json.JsonSupport.bundleBundleInfoFormat
import ml.combust.bundle.serializer.BundleSerializer
import spray.json._

import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.{Try, Using}

/**
  * Created by hollinwilkins on 12/24/16.
  */
object BundleFile {
  def load[Context <: HasBundleRegistry](uri: String)
                                        (implicit context: Context): BundleFile = {
    load(new URI(uri))
  }

  def load[Context <: HasBundleRegistry](uri: URI)
                                        (implicit context: Context): BundleFile = {
      uri.getScheme match {
        case "file" => apply(uri)
        case "jar" => apply(uri)
        case _ =>
          // look in the bundle registry for a file system
          apply(context.bundleRegistry.fileSystemForUri(uri).load(uri).get)
      }
  }

  implicit def apply(uri: String): BundleFile = {
    apply(new URI(unbackslash(uri)))
  }

  implicit def apply(path: Path): BundleFile = {
    if(path.getFileName.toString.endsWith("zip")) {
      apply(s"jar:${path.toUri}")
    } else {
      apply(path.toUri)
    }
  }

  implicit def apply(file: File): BundleFile = {
    val uri: String = if (file.getPath.endsWith(".zip")) {
      s"jar:${file.toURI.toString}"
    } else {
      file.toURI.toString
    }

    apply(uri)
  }

  implicit def apply(uri: URI): BundleFile = {
    val env = Map("create" -> "true").asJava
    val uriSafe = new URI(unbackslash(uri.toString))

    uriSafe.getScheme match {
      case "file" =>
        apply(FileSystems.getDefault, FileSystems.getDefault.getPath(uriSafe.getPath))
      case "jar" =>
        // handle resource in JAR path
        val (filesystemUri: URI, path: String) = if (uriSafe.toString.contains("!")) {
          val uriParts: Array[String] = uriSafe.toString.split("!")
          (new URI(uriParts(0)), uriParts(1))
        }
        else {
          (uriSafe, "/")
        }

        val zfs = FileSystems.newFileSystem(filesystemUri, env)
        apply(zfs, zfs.getPath(path))
    }
  }

  def apply[Context <: HasBundleRegistry](fs: BundleFileSystem, uri: URI): BundleFile = {
    // Copy contents from the bundle file system to the local file system
    apply(fs.load(uri).get)
  }

  /** Replace all backslashes with forward slashes, to handle Windows file paths in URI construction
    *
    * @param uri String representing a URI
    * @return String containing no backslashes.
    */
  private def unbackslash(uri: String): String = {
    uri.replace('\\', '/')
  }
}

case class BundleFile(fs: FileSystem,
                      path: Path) extends Closeable {
  /** Read bundle definition from the path.
    *
    * @return bundle definition
    */
  def readInfo(): Try[BundleInfo] = {
    val bundleJson = fs.getPath(path.toString, Bundle.bundleJson)
    Try(new String(Files.readAllBytes(bundleJson), "UTF-8").parseJson.convertTo[ml.bundle.Bundle]).
      map(BundleInfo.fromBundle)
  }

  def writeNote(name: String, note: String): Try[String] = {
    Files.createDirectories(fs.getPath(path.toString, "notes"))
    Using(Files.newOutputStream(fs.getPath(path.toString, "notes", name))) {
      out => {
        out.write(note.getBytes)
        note
      }
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

  def load[Context <: HasBundleRegistry, Transformer <: AnyRef]()
                                                               (implicit context: Context): Try[Bundle[Transformer]] = {
    BundleSerializer(context, this).read[Transformer]()
  }

  override def finalize(): Unit = {
    super.finalize()
    close()
  }

  override def close(): Unit = {
    // closing some file systems, like Unix, raises an
    // unsupported operation error
    Try(fs.close())
  }
}
