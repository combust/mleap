package ml.combust.bundle.v07.converter

import java.nio.file.{FileSystem, Path}

/**
  * Created by hollinwilkins on 1/27/18.
  */
case class ConverterFile(fs: FileSystem,
                         path: Path) {
  def updatePath(name: String): ConverterFile = {
    ConverterFile(fs, fs.getPath(path.toString, name))
  }

  /** Get a file in the current bundle folder.
    *
    * @param name name of the file
    * @return file in the bundle
    */
  def file(name: String): Path = fs.getPath(path.toString, name)
}
