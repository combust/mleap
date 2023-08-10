package ml.combust.mleap

import sbt.Keys._
import sbt._
import sbtprotoc.ProtocPlugin.autoImport.PB

object Protobuf {
  lazy val bundleSettings = Seq(Compile / PB.targets := Seq(
    PB.gens.java -> (Compile / sourceManaged).value,
    scalapb.gen(flatPackage = true, javaConversions = true) -> (Compile / sourceManaged).value),
    Compile / PB.includePaths ++= Seq(file("bundle-protobuf/bundle")),
    Compile / PB.protoSources := Seq(file("bundle-protobuf/bundle")))

  lazy val grpcSettings = Seq(Compile / PB.targets := Seq(scalapb.gen(flatPackage = true) -> (Compile / sourceManaged).value),
    Compile / PB.includePaths ++= Seq(file("bundle-protobuf/mleap"), file("bundle-protobuf/grpc")),
    Compile / PB.protoSources := Seq(file("bundle-protobuf/mleap"), file("bundle-protobuf/grpc")))

  lazy val springBootSettings = Seq(Compile / PB.targets := Seq(
    PB.gens.java -> (Compile / sourceManaged).value,
    scalapb.gen(flatPackage = true, javaConversions = true) -> (Compile / sourceManaged).value),
    Compile / PB.includePaths ++= Seq(file("bundle-protobuf/mleap")),
    Compile / PB.protoSources := Seq(file("bundle-protobuf/mleap")))
}
