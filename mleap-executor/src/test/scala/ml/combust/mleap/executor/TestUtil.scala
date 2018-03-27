package ml.combust.mleap.executor

import java.io.File
import java.net.URI

import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import ml.combust.mleap.runtime.serialization.FrameReader

object TestUtil {
  lazy val rfUri: URI = {
    getClass.getClassLoader.getResource("models/airbnb.model.rf.zip").toURI
  }

  lazy val lrUri: URI = {
    getClass.getClassLoader.getResource("models/airbnb.model.lr.zip").toURI
  }

  lazy val faultyFrame: DefaultLeapFrame = {
    DefaultLeapFrame(StructType(), Seq())
  }

  lazy val frame: DefaultLeapFrame = {
    FrameReader().read(new File(getClass.getClassLoader.getResource("leap_frame/frame.airbnb.json").getFile)).get
  }
}
