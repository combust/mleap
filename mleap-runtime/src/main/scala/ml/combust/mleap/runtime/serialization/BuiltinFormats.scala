package ml.combust.mleap.runtime.serialization

import java.nio.charset.Charset

/**
  * Created by hollinwilkins on 11/2/16.
  */
object BuiltinFormats {
  val charset = Charset.forName("UTF-8")

  val json = classOf[ml.combust.mleap.json.DefaultFrameReader].getPackageName
  val binary = classOf[ml.combust.mleap.binary.DefaultFrameReader].getPackageName
  val avro = "ml.combust.mleap.avro"
}
