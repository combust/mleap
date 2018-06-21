package ml.combust.mleap.springboot

import java.io.File
import java.net.URI
import java.nio.file.{Files, StandardCopyOption}

import ml.combust.mleap.core.types.{ScalarType, StructField, StructType}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

object TestUtil {

  lazy val demoUri = getClass.getClassLoader.getResource("demo.zip").toURI

  lazy val validFrame = DefaultLeapFrame(
    StructType(Seq(StructField("demo:a", ScalarType.Double),
      StructField("demo:c", ScalarType.Double),
      StructField("demo:d", ScalarType.Double))).get,
    Seq(Row(44.5, 22.1, 98.2)))

  lazy val incompleteFrame = DefaultLeapFrame(
    StructType(Seq(StructField("demo:a", ScalarType.Double),
      StructField("demo:d", ScalarType.Double))).get,
    Seq(Row(44.5, 98.2)))

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
