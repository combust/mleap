package ml.combust.mleap

import sbt.Keys._
import sbt._
import sbtprotoc.ProtocPlugin.autoImport.PB

object Protobuf {
  lazy val bundleSettings = Seq(PB.targets in Compile := Seq(PB.gens.java -> (sourceManaged in Compile).value),
    PB.includePaths in Compile := Seq(file("bundle-protobuf")),
    PB.protoSources in Compile := Seq(file("bundle-protobuf")))
}
