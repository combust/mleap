package ml.combust.mleap.runtime.test

import ml.combust.bundle.BundleRegistry
import ml.combust.bundle.custom.CustomType
import spray.json.RootJsonFormat
import spray.json.DefaultJsonProtocol._

/**
  * Created by hollinwilkins on 10/30/16.
  */
case class MyCustomObject(name: String)

class MyCustomType(bundleRegistry: BundleRegistry = BundleRegistry("ml.combust.mleap.registry.default")) extends CustomType[MyCustomObject] {
  override val klazz: Class[MyCustomObject] = classOf[MyCustomObject]

  override def name: String = "my_custom_object"

  override def format: RootJsonFormat[MyCustomObject] = jsonFormat1(MyCustomObject)
}
