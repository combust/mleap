package ml.combust.bundle.serializer

import java.io.File
import java.util.zip.{ZipInputStream, ZipOutputStream}

/**
  * Created by hollinwilkins on 9/11/16.
  */
@deprecated("Prefer ml.combust.bundle.util.FileUtil object.")
case class FileUtil() {
  import ml.combust.bundle.util.{FileUtil => FileUtils}
  @deprecated("use FileUtil.rmRF(Path).")
  def rmRF(path: File): Array[(String, Boolean)] = {
    FileUtils.rmRF(path.toPath)
  }

  @deprecated("use extract(Path, Path).")
  def extract(source: File, dest: File): Unit = {
    FileUtils.extract(source.toPath, dest.toPath)
  }

  @deprecated("use extract(ZipInputStream, Path).")
  def extract(in: ZipInputStream, dest: File): Unit = {
    FileUtils.extract(in, dest.toPath)
  }

  @deprecated("use FileUtil.extract(Path, Path).")
  def zip(source: File, dest: File): Unit = {
    FileUtils.zip(source.toPath, dest.toPath)
  }

  @deprecated("use FileUtil.extract(Path, ZipOutputStream).")
  def zip(source: File, dest: ZipOutputStream): Unit = FileUtils.zip(source.toPath, source.toPath, dest)

  @deprecated("use FileUtil.extract(Path, Path, ZipOutputStream).")
  def zip(base: File, source: File, dest: ZipOutputStream): Unit = {
    FileUtils.zip(base.toPath, source.toPath, dest)
  }

}