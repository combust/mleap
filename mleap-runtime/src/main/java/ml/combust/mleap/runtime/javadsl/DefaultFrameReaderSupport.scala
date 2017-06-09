package ml.combust.mleap.runtime.javadsl

import ml.combust.mleap.json.DefaultFrameReader
import ml.combust.mleap.runtime.{DefaultLeapFrame, MleapContext}

import scala.collection.JavaConverters._

class DefaultFrameReaderSupport {

  private val frameReader = new DefaultFrameReader()

  def fromBytes(iterable: java.lang.Iterable[Byte], context: MleapContext): DefaultLeapFrame = {
    frameReader.fromBytes(iterable.asScala.toArray)(context).get
  }
}