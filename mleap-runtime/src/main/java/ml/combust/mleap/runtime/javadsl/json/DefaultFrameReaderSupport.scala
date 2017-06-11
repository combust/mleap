package ml.combust.mleap.runtime.javadsl.json

import ml.combust.mleap.json.DefaultFrameReader
import ml.combust.mleap.runtime.{DefaultLeapFrame, MleapContext}

import scala.collection.JavaConverters._
import scala.util.Try

class DefaultFrameReaderSupport {

  private val frameReader = new DefaultFrameReader()

  def fromBytes(iterable: java.lang.Iterable[Byte], context: MleapContext): Try[DefaultLeapFrame] = {
    frameReader.fromBytes(iterable.asScala.toArray)(context)
  }
}