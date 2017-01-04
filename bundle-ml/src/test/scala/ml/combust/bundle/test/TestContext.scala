package ml.combust.bundle.test

import ml.combust.bundle.{BundleRegistry, HasBundleRegistry}

/**
  * Created by hollinwilkins on 12/24/16.
  */
case class TestContext(bundleRegistry: BundleRegistry) extends HasBundleRegistry
