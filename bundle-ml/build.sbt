import ml.combust.mleap.{Dependencies, Common, Protobuf}

Common.defaultBundleSettings
Protobuf.bundleSettings
Dependencies.bundleMl

libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
