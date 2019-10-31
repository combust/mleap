import ml.combust.mleap.{Dependencies, Common}

Common.defaultMleapSettings
Dependencies.sparkExtension

val writeRuntimeClasspathToFile = taskKey[Unit]("Writes runtime classpath to a file")

// this works in all cases: especially also if sources are compiled due to `sbt test` this task is run
writeRuntimeClasspathToFile <<= Def.task {
  // sbt complains about deprecated syntax, but unfortunately := can't be used with triggeredBy with sbt 0.13.
  // Fix for triggeredBy is only available in 0.13.14, see https://github.com/sbt/sbt/issues/1444

  val file = reflect.io.File(f"mleap-spark-extension/target/classpath-runtime_${(scalaVersion in ThisBuild).value}.txt")
  println(f"writeRuntimeClasspathToFile -> ${file.toAbsolute}")

  val classPathString = Path.makeString((dependencyClasspath in Runtime).value.map(_.data))
  println(f"classPathString: ${classPathString}")

  file.writeAll(classPathString)
}.triggeredBy(compile in Compile)
