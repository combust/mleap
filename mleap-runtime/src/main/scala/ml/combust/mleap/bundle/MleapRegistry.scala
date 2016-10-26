package ml.combust.mleap.bundle

import ml.combust.bundle.serializer.BundleRegistry

/**
  * Created by hollinwilkins on 8/22/16.
  */
object MleapRegistry {
  lazy val defaultRegistry: BundleRegistry = create()

  def create(): BundleRegistry = BundleRegistry("mleap")
}
