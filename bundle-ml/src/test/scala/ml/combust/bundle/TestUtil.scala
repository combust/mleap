package ml.combust.bundle

import java.nio.file.{Files, Path}

/**
  * Created by hollinwilkins on 8/23/16.
  */
object TestUtil {
  val baseDir: Path = Files.createTempDirectory("mleap-bundle-tests").toAbsolutePath
}

