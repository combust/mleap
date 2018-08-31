import ml.combust.mleap.{Common, Dependencies, Protobuf}
import sbtassembly.MergeStrategy

Dependencies.springBootServing
Common.defaultMleapSettings
Protobuf.springBootSettings

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("dependencies" :: Nil)
      => MergeStrategy.discard
      case "services" :: xs => MergeStrategy.filterDistinctLines
      case (x :: Nil) if x.contains("spring") || x == "web-fragment.xml" => MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.deduplicate
    }
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.deduplicate
}

mainClass in assembly := Some("ml.combust.mleap.springboot.JavaStarter")