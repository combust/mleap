import ml.combust.mleap.Common
import sbtassembly.AssemblyPlugin.autoImport.ShadeRule

Common.defaultMleapSettings
Common.noPublishSettings

enablePlugins(AssemblyPlugin)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyShadeRules in assembly := Seq(
  "spray.json.**",
  "com.google.protobuf.**",
  "com.trueaccord.**",
  "au.com.**",
  "com.github.**",
  "com.typesafe.**",
  "edu.emory.**",
  "fastparse.**",
  "google.protobuf.**",
  "machinist.**",
  "macrocompat.**",
  "org.apache.commons.**",
  "org.netlib.**",
  "org.j_paine.**",
  "resource.**",
  "scalapb.**",
  "scalaxy.**",
  "shapeless.**",
  "spire.**",
  "sourcecode.**",
  "buildinfo.**",
  "ai.h2o.**",
  "biz.k11i.**",
  "com.esotericsoftware.**",
  "net.jafama.**",
  "org.objectweb.**",
  "org.objenesis.**",
  "algebra.**",
  "breeze.**",
  "cats.**",
  "org.jtransforms.**",
  "pl.edu.icm.jlargearrays.**",
  "scala.collection.compat.**", // shading the library https://github.com/scala/scala-collection-compat, but not scala collection
  "scala.jdk.**", // this is also introduced by scala-collection-compat
  "scala.util.**",
  "scala.annotation.**",
  "org.apache.log4j.**"
).map { pattern =>
  ShadeRule.rename(pattern -> "ml.combust.mleap.shaded.@0").inAll
} :+ ShadeRule.zap("org.slf4j.**").inAll
