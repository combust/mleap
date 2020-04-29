import ml.combust.mleap.{Dependencies, Common}

Common.defaultMleapSettings
Dependencies.spark

unmanagedSourceDirectories in Compile += {
  if (Dependencies.sparkVersion.startsWith("3.0.")) {
    baseDirectory.value / "src" / "spark-shims" / "spark-3.0"
  } else {
    baseDirectory.value / "src" / "spark-shims" / "spark-2.4"
  }
}
