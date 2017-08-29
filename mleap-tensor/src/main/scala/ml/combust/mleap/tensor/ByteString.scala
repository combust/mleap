package ml.combust.mleap.tensor

/**
  * Created by hollinwilkins on 1/20/17.
  */
case class ByteString(bytes: Array[Byte]) {
  def size: Int = bytes.length

  override def equals(obj: scala.Any): Boolean = obj match {
    case obj: ByteString => bytes sameElements obj.bytes
    case _ => false
  }

  override def hashCode(): Int = bytes.toSeq.hashCode()
}
