package ml.combust.mleap.tensor

import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, _}

import scala.reflect.ClassTag

/**
  * Created by hollinwilkins on 1/15/17.
  */
trait JsonSupport {
  implicit def mleapArrayFormat[T: JsonFormat: ClassTag]: RootJsonFormat[Array[T]] = new RootJsonFormat[Array[T]] {
    val base = implicitly[JsonFormat[T]]

    override def write(obj: Array[T]): JsValue = {
      JsArray(obj.map(base.write): _*)
    }

    override def read(json: JsValue): Array[T] = json match {
      case json: JsArray =>
        val elements = json.elements
        val size = elements.size
        val values = new Array[T](size)
        (0 until size).foreach(i => values(i) = base.read(elements(i)))
        values
      case _ => deserializationError("invalid array")
    }
  }

  implicit def mleapDenseTensorFormat[T: JsonFormat: ClassTag]: RootJsonFormat[DenseTensor[T]] = jsonFormat3(DenseTensor[T])
  implicit def mleapSparseTensorFormat[T: JsonFormat: ClassTag]: RootJsonFormat[SparseTensor[T]] = jsonFormat4(SparseTensor[T])
  implicit def mleapTensorFormat[T: JsonFormat: ClassTag]: RootJsonFormat[Tensor[T]] = new RootJsonFormat[Tensor[T]] {
    override def write(obj: Tensor[T]): JsValue = obj match {
      case obj: DenseTensor[_] => obj.asInstanceOf[DenseTensor[T]].toJson
      case obj: SparseTensor[_] => obj.asInstanceOf[SparseTensor[T]].toJson
    }

    override def read(json: JsValue): Tensor[T] = json match {
      case json: JsObject =>
        if(json.fields.contains("indices")) {
          mleapSparseTensorFormat[T].read(json)
        } else {
          mleapDenseTensorFormat[T].read(json)
        }
      case _ => deserializationError("invalid tensor")
    }
  }
}
object JsonSupport extends JsonSupport