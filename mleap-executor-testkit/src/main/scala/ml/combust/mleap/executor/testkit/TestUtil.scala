package ml.combust.mleap.executor.testkit

import java.io.File
import java.net.URI
import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, RowTransformer, Transformer}
import ml.combust.mleap.runtime.serialization.FrameReader

import java.nio.file.Path
import scala.util.Using

object TestUtil {
  private val classLoader = getClass.getClassLoader
  lazy val rfUri: URI = {
    classLoader.getResource("models/airbnb.model.rf.zip").toURI
  }

  lazy val lrUri: URI = {
    classLoader.getResource("models/airbnb.model.lr.zip").toURI
  }

  lazy val faultyFrame: DefaultLeapFrame = {
    DefaultLeapFrame(StructType(), Seq())
  }

  lazy val frame: DefaultLeapFrame = {
    FrameReader().read(Path.of(classLoader.getResource("leap_frame/frame.airbnb.json").toURI)).get
  }

  lazy val rfBundle : Bundle[Transformer] = loadBundle(rfUri)

  lazy val lrBundle : Bundle[Transformer] = loadBundle(lrUri)


  lazy val rfRowTransformer: RowTransformer = rfBundle.root.transform(RowTransformer(frame.schema)).get
  private def loadBundle(uri: URI): Bundle[Transformer] =
    Using(BundleFile(new File(uri))) { bundle =>
      bundle.loadMleapBundle()
    }.flatten.get
}
