package ml.combust.mleap.runtime.serialization

import java.nio.charset.Charset

/**
  * Created by hollinwilkins on 11/2/16.
  */
object BuiltinFormats {
  val charset = Charset.forName("UTF-8")
  val json = "ml.combust.mleap.json"
  val binary = "ml.combust.mleap.binary"
  val avro = "ml.combust.mleap.avro"
}
