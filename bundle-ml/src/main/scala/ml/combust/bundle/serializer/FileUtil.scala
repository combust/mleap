package ml.combust.bundle.serializer

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipInputStream, ZipOutputStream}

import resource._

/**
  * Created by hollinwilkins on 9/11/16.
  */
case class FileUtil() {
  def rmRF(path: File): Array[(String, Boolean)] = {
    Option(path.listFiles).map(_.flatMap(f => rmRF(f))).getOrElse(Array()) :+ (path.getPath -> path.delete)
  }

  def extract(source: File, dest: File): Unit = {
    dest.mkdirs()
    for(in <- managed(new ZipInputStream(new FileInputStream(source)))) {
      extract(in, dest)
    }
  }

  def extract(in: ZipInputStream, dest: File): Unit = {
    dest.mkdirs()
    val buffer = new Array[Byte](1024 * 1024)

    var entry = in.getNextEntry
    while(entry != null) {
      if(entry.isDirectory) {
        new File(dest, entry.getName).mkdirs()
      } else {
        val filePath = new File(dest, entry.getName)
        for(out <- managed(new FileOutputStream(filePath))) {
          var len = in.read(buffer)
          while(len > 0) {
            out.write(buffer, 0, len)
            len = in.read(buffer)
          }
        }
      }
      entry = in.getNextEntry
    }
  }

  def zip(source: File, dest: File): Unit = {
    for(out <- managed(new ZipOutputStream(new FileOutputStream(dest)))) {
      zip(source, out)
    }
  }

  def zip(source: File, dest: ZipOutputStream): Unit = zip(source, source, dest)

  def zip(base: File, source: File, dest: ZipOutputStream): Unit = {
    val buffer = new Array[Byte](1024 * 1024)

    for(files <- Option(source.listFiles);
        file <- files) {
      val name = file.toString.substring(base.toString.length + 1)

      if(file.isDirectory) {
        dest.putNextEntry(new ZipEntry(s"$name/"))
        zip(base, file, dest)
      } else {
        dest.putNextEntry(new ZipEntry(name))

        for (in <- managed(new FileInputStream(file))) {
          var read = in.read(buffer)
          while (read > 0) {
            dest.write(buffer, 0, read)
            read = in.read(buffer)
          }
        }
      }
    }
  }
}
