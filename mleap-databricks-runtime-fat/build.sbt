import ml.combust.mleap.Common

Common.defaultMleapSettings

enablePlugins(AssemblyPlugin)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("spray.json.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("com.google.protobuf.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("breeze.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("com.trueaccord.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("au.com.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("com.github.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("com.typesafe.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("edu.emory.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("fastparse.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("google.protobuf.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("machinist.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("macrocompat.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("org.apache.commons.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("org.netlib.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("org.j_paine.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("resource.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("scalapb.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("scalaxy.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("shapeless.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("spire.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("sourcecode.**" -> "ml.combust.mleap.shaded.@0").inAll,
  ShadeRule.rename("buildinfo.**" -> "ml.combust.mleap.shaded.@0").inAll,

  ShadeRule.zap("org.slf4j.**").inAll
)
