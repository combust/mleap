package ml.combust.mleap.runtime.serialization.json

import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.types.StructType
import spray.json._

/**
  * Created by hollinwilkins on 10/31/16.
  */
case class RowFormat(schema: StructType) extends RootJsonFormat[Row] {
  val serializers: Seq[JsonFormat[Any]] = schema.fields.map(_.dataType).
    map(DatasetFormat.serializer).
    map(_.asInstanceOf[JsonFormat[Any]])

  override def write(obj: Row): JsValue = {
    var i = 0
    val values = serializers.map {
      s =>
        val v = s.write(obj(i))
        i += 1
        v
    }
    JsArray(values: _*)
  }

  override def read(json: JsValue): Row = json match {
    case json: JsArray =>
      var i = 0
      val values = serializers.map {
        s =>
          val v = s.read(json.elements(i))
          i += 1
          v
      }
      Row(values: _*)
    case _ => deserializationError("invalid row")
  }
}
