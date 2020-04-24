import ml.combust.mleap.{Dependencies, Common}

Common.defaultMleapSettings
Dependencies.sparkExtension

val printClassPath = taskKey[Unit]("Prints runtime classpath")

printClassPath <<= Def.task {
  val classPathString = Path.makeString((dependencyClasspath in Runtime).value.map(_.data))
  println(f"${classPathString}")
}
