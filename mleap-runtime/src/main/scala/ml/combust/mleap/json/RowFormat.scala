package ml.combust.mleap.json

import ml.combust.mleap.core.types._
import ml.combust.mleap.json.JsonSupport.{BundleByteStringFormat, mleapTensorFormat}
import ml.combust.mleap.runtime.frame.Row
import ml.combust.mleap.tensor.ByteString
import spray.json.DefaultJsonProtocol._
import spray.json._

/**
  * Created by hollinwilkins on 10/31/16.
  */
object RowFormat {
  def maybeNullableFormat[T](base: JsonFormat[T],
                             isNullable: Boolean): JsonFormat[_] = {
    if(isNullable) {
      optionFormat(base)
    } else {
      base
    }
  }

  def listSerializer(lt: ListType): JsonFormat[_] = {
    maybeNullableFormat(seqFormat(basicSerializer(lt.base, isNullable = false)), lt.isNullable)
  }

  def mapSerializer(mt: MapType): JsonFormat[_] = {
    maybeNullableFormat(
      mapFormat(
        basicSerializer(mt.key, isNullable = false),
        basicSerializer(mt.base, isNullable = false)
      ),
      mt.isNullable
    )
  }

  def tensorSerializer(tt: TensorType): JsonFormat[_] = {
    val isNullable = tt.isNullable

    tt.base match {
      case BasicType.Boolean => maybeNullableFormat(mleapTensorFormat[Boolean], isNullable)
      case BasicType.Byte => maybeNullableFormat(mleapTensorFormat[Byte], isNullable)
      case BasicType.Short => maybeNullableFormat(mleapTensorFormat[Short], isNullable)
      case BasicType.Int => maybeNullableFormat(mleapTensorFormat[Int], isNullable)
      case BasicType.Long => maybeNullableFormat(mleapTensorFormat[Long], isNullable)
      case BasicType.Float => maybeNullableFormat(mleapTensorFormat[Float], isNullable)
      case BasicType.Double => maybeNullableFormat(mleapTensorFormat[Double], isNullable)
      case BasicType.String => maybeNullableFormat(mleapTensorFormat[String], isNullable)
      case BasicType.ByteString => maybeNullableFormat(mleapTensorFormat[ByteString], isNullable)
      case _ => serializationError(s"invalid tensor base type: ${tt.base}")
    }
  }

  def basicSerializer(base: BasicType, isNullable: Boolean): JsonFormat[_] = base match {
    case BasicType.Boolean => maybeNullableFormat(BooleanJsonFormat, isNullable)
    case BasicType.Byte => maybeNullableFormat(ByteJsonFormat, isNullable)
    case BasicType.Short => maybeNullableFormat(ShortJsonFormat, isNullable)
    case BasicType.Int => maybeNullableFormat(IntJsonFormat, isNullable)
    case BasicType.Long => maybeNullableFormat(LongJsonFormat, isNullable)
    case BasicType.Float => maybeNullableFormat(FloatJsonFormat, isNullable)
    case BasicType.Double => maybeNullableFormat(DoubleJsonFormat, isNullable)
    case BasicType.String => maybeNullableFormat(StringJsonFormat, isNullable)
    case BasicType.ByteString => maybeNullableFormat(BundleByteStringFormat, isNullable)
    case _ => serializationError(s"invalid basic type: $base")
  }

  def scalarSerializer(st: ScalarType): JsonFormat[_] = basicSerializer(st.base, st.isNullable)

  def serializer(dt: DataType): JsonFormat[_] = dt match {
    case st: ScalarType => scalarSerializer(st)
    case lt: ListType => listSerializer(lt)
    case tt: TensorType => tensorSerializer(tt)
    case mt: MapType => mapSerializer(mt)
    case _ => serializationError(s"invalid serialization type $dt")
  }
}

case class RowFormat(schema: StructType) extends RootJsonFormat[Row] {
  val serializers: Seq[(JsonFormat[Any], Boolean)] = schema.fields.map(_.dataType).
    map {
      dt =>
        val s = RowFormat.serializer(dt).asInstanceOf[JsonFormat[Any]]
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
