import ml.combust.mleap.{Common, Dependencies}

resolvers += Resolver.mavenLocal

Common.defaultMleapSettings
Common.noPublishSettings

Dependencies.databricksRuntimeTestkit
