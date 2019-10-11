import ml.combust.mleap.{Dependencies, Common}

resolvers += "Spark snapshot repository" at "https://repository.apache.org/snapshots/"

Common.defaultMleapSettings
Dependencies.sparkBase
