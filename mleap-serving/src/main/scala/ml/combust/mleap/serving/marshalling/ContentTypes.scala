package ml.combust.mleap.serving.marshalling

import akka.http.scaladsl.model.MediaType.NotCompressible
import akka.http.scaladsl.model.{ContentType, MediaType}

/**
  * Created by hollinwilkins on 1/30/17.
  */
object MediaTypes {
  val `application/binary` = MediaType.customBinary("application", "binary", NotCompressible)
  val `application/avro` = MediaType.customBinary("application", "avro", NotCompressible)
}

object ContentTypes {
  val `application/binary` = ContentType.Binary(MediaTypes.`application/binary`)
  val `application/avro` = ContentType.Binary(MediaTypes.`application/avro`)
}
