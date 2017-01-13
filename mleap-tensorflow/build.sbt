import ml.combust.mleap.{Dependencies, Common}

Common.defaultMleapSettings
Dependencies.tensorflow

javaOptions in Test ++= sys.env.get("TENSORFLOW_JNI").map {
  jniPath => Seq(s"-Djava.library.path=$jniPath")
}.getOrElse(Seq())

sys.env.get("MLEAP_TENSORFLOW_TEST") match {
  case Some("true") => Seq()
  case _ => Seq(test := false)
}