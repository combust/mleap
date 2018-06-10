package ml.combust.mleap.grpc.server

import com.google.protobuf.ByteString
import ml.combust.mleap.executor.TagBytes

object ByetStringTagBytes {
  implicit object ByetStringTagBytes extends TagBytes[ByteString] {
    override def toBytes(tag: ByteString): Array[Byte] = tag.toByteArray

    override def fromBytes(bytes: Array[Byte]): ByteString = ByteString.copyFrom(bytes)
  }
}
