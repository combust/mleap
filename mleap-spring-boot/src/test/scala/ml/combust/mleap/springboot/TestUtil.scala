package ml.combust.mleap.springboot

import java.io.File
import java.net.URI
import java.nio.file.{Files, StandardCopyOption}

object TestUtil {

  lazy val demoUri = getClass.getClassLoader.getResource("demo.zip").toURI

  def getBundle(uri: URI, createTmp: Boolean): URI = {
    if (createTmp) {
      val tmpFile = Files.createTempFile("demo", ".bundle.zip")
      val file = new File(uri.getPath).toPath
      Files.copy(file, tmpFile, StandardCopyOption.REPLACE_EXISTING)
      tmpFile.toFile.deleteOnExit()
      tmpFile.toUri
    } else {
      uri
    }
  }
}
