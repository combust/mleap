package ml.combust.mleap.json

import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.Row
import spray.json._

/**
  * Created by hollinwilkins on 10/31/16.
  */
case class RowFormat(schema: StructType) extends RootJsonFormat[Row] {
  val serializers: Seq[(JsonFormat[Any], Boolean)] = schema.fields.map(_.dataType).
    map {
      dt =>
        val s = DatasetFormat.serializer(dt).asInstanceOf[JsonFormat[Any]]
        (s, dt.isNullable)
    }

  override def write(obj: Row): JsValue = {
    var i = 0
    val values = serializers.map {
      case (s, isNullable) =>
        val v = if(isNullable) {
          s.write(obj.option(i))
        } else {
          s.write(obj.getRaw(i))
        }
        i += 1
        v
    }
    JsArray(values: _*)
  }

  override def read(json: JsValue): Row = json match {
    case json: JsArray =>
      var i = 0
      val values = serializers.map {
        case (s, isNullable) =>
          val v = if(isNullable) {
            s.read(json.elements(i)).asInstanceOf[Option[Any]].orNull
          } else {
            s.read(json.elements(i))
          }
          i += 1
          v
      }
      Row(values: _*)
    case _ => deserializationError("invalid row")
  }
}
