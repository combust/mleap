import ml.combust.mleap.{Dependencies, Common}

Common.defaultMleapSettings
Dependencies.xgboostSpark

Test / javaOptions ++= sys.env.get("XGBOOST_JNI").map {
  jniPath => Seq(s"-Djava.library.path=$jniPath")
}.getOrElse(Seq())
