import ml.combust.mleap.{Dependencies, Common, Protobuf}

Dependencies.grpc
Common.defaultMleapServingSettings
Protobuf.grpcSettings

libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
