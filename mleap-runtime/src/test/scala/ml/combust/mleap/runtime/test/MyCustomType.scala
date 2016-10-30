package ml.combust.mleap.runtime.test

import ml.combust.mleap.runtime.types.CustomType
import spray.json._
import DefaultJsonProtocol._

/**
  * Created by hollinwilkins on 10/26/16.
  */
case class MyCustomObject(name: String)

class MyCustomType extends CustomType[MyCustomObject] {
  override val klazz: Class[MyCustomObject] = classOf[MyCustomObject]

  override val name: String = "my_custom_object"

  override val format: JsonFormat[MyCustomObject] = jsonFormat1(MyCustomObject)
}
