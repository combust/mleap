import ml.combust.mleap.{Dependencies, Common}

Common.defaultMleapSettings
Dependencies.sparkExtension

val writeRuntimeClasspathToFile = taskKey[Unit]("Writes runtime classpath to a file")

writeRuntimeClasspathToFile := {
  val file = reflect.io.File("mleap-spark-extension/target/classpath-runtime.txt")
  println(f"writeRuntimeClasspathToFile -> ${file.toAbsolute}")
  file
    .writeAll(Path.makeString((dependencyClasspath in Runtime)
      .value.map(_.data)))
}

// TODO this didn't work exactly as intended.. requires explicitly calling 'sbt compile'. eg. 'sbt test' skips this
//  how to make sbt always run this always, for example as part of 'sbt test'?
// run writeRuntimeClasspathToFile as part of 'compile' of this module
//compile := {(compile in Compile) dependsOn writeRuntimeClasspathToFile}.value
