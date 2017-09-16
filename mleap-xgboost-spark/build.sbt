import ml.combust.mleap.{Dependencies, Common}

resolvers += Resolver.mavenLocal

Common.defaultMleapSettings
Dependencies.xgboostSpark

javaOptions in Test ++= sys.env.get("XGBOOST_JNI").map {
  jniPath => Seq(s"-Djava.library.path=$jniPath")
}.getOrElse(Seq())
