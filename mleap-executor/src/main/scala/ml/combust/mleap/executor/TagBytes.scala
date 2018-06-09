package ml.combust.mleap.executor

import java.nio.ByteBuffer
import java.util.UUID

trait TagBytes[Tag] {
  def toBytes(tag: Tag): Array[Byte]
  def fromBytes(bytes: Array[Byte]): Tag
}

object TagBytes {
  implicit object UUIDTagBytes extends TagBytes[UUID] {
    override def toBytes(tag: UUID): Array[Byte] = {
      val buffer = ByteBuffer.allocate(16)
      val lBuffer = buffer.asLongBuffer()
      lBuffer.put(tag.getLeastSignificantBits)
      lBuffer.put(tag.getMostSignificantBits)
      buffer.array()
    }

    override def fromBytes(bytes: Array[Byte]): UUID = {
      UUID.nameUUIDFromBytes(bytes)
    }
  }
}
