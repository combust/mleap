import ml.combust.mleap.{Dependencies, Common}

Common.defaultMleapSettings
Dependencies.sparkExtension

val writeRuntimeClasspathToFile = taskKey[Unit]("Writes runtime classpath to a file")

// TODO This produces the same output on command line
//
//  sbt --error "export mleap-spark-extension/runtime:fullClasspath" > fullClasspath.txt
//
// (--error is needed to disable info logging printed into the the file as well)
//
// TODO I couldn't find how to run the same "command" as scala from code here, but I guess it should be doable.
//  That would allow simplifying the "impl" of writeRuntimeClasspathToFile task below.

// this works in all cases: especially also if sources are compiled due to `sbt test` this task is run
writeRuntimeClasspathToFile <<= Def.task {
  // sbt complains about deprecated syntax, but unfortunately := can't be used with triggeredBy with sbt 0.13.
  // Fix for triggeredBy is only available in 0.13.14, see https://github.com/sbt/sbt/issues/1444
  val file = reflect.io.File("mleap-spark-extension/target/classpath-runtime.txt")
  println(f"writeRuntimeClasspathToFile -> ${file.toAbsolute}")
  val classPathString = Path.makeString((dependencyClasspath in Runtime)
    .value.map(_.data))
  println(f"classPathString: ${classPathString}")
  file.writeAll(classPathString)
}.triggeredBy(compile in Compile)
