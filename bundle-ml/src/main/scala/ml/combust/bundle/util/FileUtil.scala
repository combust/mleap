package ml.combust.bundle.util

import java.io.{IOException, InputStream, OutputStream}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, FileSystems, Path, SimpleFileVisitor}
import java.util.Comparator
import java.util.stream.Collectors
import java.util.zip.{ZipEntry, ZipInputStream, ZipOutputStream}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try, Using}
/**
  * Created by hollinwilkins on 12/24/16.
  */
object FileUtil {
  private val bufferSize = 1024 * 1024

  def rmRf(path: Path): Unit = {
    Files.walkFileTree(path, new SimpleFileVisitor[Path]() {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(file)
        FileVisitResult.CONTINUE
      }

      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
      }
    })
  }

  def rmRF(toRemove: Path): Array[(String, Boolean)] = {
    def removeElement(path: Path): (String, Boolean) = {
      val result = Try {
        Files.deleteIfExists(path)
      } match {
        case Failure(_) => false
        case Success(value) => value
      }
      path.toAbsolutePath.toString -> result
    }

    if (Files.isDirectory(toRemove)) {
      Using(Files.walk(toRemove)) {
        paths =>
          paths
            .sorted(Comparator.reverseOrder())
            .collect(Collectors.toList())
            .asScala
            .map(removeElement(_))
            .toArray
      }.getOrElse(Array.empty)
    } else {
      Array(removeElement(toRemove))
    }
  }

  def extract(source: Path, dest: Path): Unit = {
    Files.createDirectories(dest)
    Using(new ZipInputStream(Files.newInputStream(source))) {
      in => extract(in, dest)
    }
  }

  def extract(in: ZipInputStream, dest: Path): Unit = {
    Files.createDirectories(dest)

    var entry = in.getNextEntry
    while (entry != null) {
      val filePath = dest.resolve(entry.getName)
      if (entry.isDirectory) {
        Files.createDirectories(filePath)
      } else {
        val destCanonical = dest.toRealPath()
        val entryCanonical = filePath.toAbsolutePath().normalize()
        if (!entryCanonical.startsWith(destCanonical + FileSystems.getDefault().getSeparator())) {
          throw new Exception("Entry is outside of the target dir: " + entry.getName)
        }
        Using(Files.newOutputStream(filePath)) {
          out => writeData(in, out)
        }
      }
      entry = in.getNextEntry
    }
  }

  def zip(source: Path, dest: Path): Unit = {
    Using(new ZipOutputStream(Files.newOutputStream(dest))) {
      out => zip(source, out)
    }
  }

  def zip(source: Path, dest: ZipOutputStream): Unit = zip(source, source, dest)


  def zip(base: Path, source: Path, dest: ZipOutputStream): Unit = {
    Using(Files.walk(source)) { paths =>
      paths.forEachOrdered(
        path => {
          val name = base.relativize(path).toString
          if (!Files.isDirectory(path)) {
            dest.putNextEntry(new ZipEntry(name))
            Using(Files.newInputStream(path)) {
              in => writeData(in, dest)
            }
          } else {
            dest.putNextEntry(new ZipEntry(s"$name/"))
            zip(base, path, dest)
          }
        })
    }
  }

  private def writeData(in: InputStream, out: OutputStream): Unit = {
    val buffer = new Array[Byte](bufferSize)
    var len = in.read(buffer)
    while (len > 0) {
      out.write(buffer, 0, len)
      len = in.read(buffer)
    }
  }
}
