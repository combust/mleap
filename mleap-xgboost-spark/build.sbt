import ml.combust.mleap.{Dependencies, Common}

resolvers += Resolver.mavenLocal

Common.defaultMleapSettings
Dependencies.xgboostSpark

assemblyMergeStrategy in assembly := {
  case x: String if x.contains("UnusedStubClass") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

javaOptions in Test ++= sys.env.get("XGBOOST_JNI").map {
  jniPath => Seq(s"-Djava.library.path=$jniPath")
}.getOrElse(Seq())
