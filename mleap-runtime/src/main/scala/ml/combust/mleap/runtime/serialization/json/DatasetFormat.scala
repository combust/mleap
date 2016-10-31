package ml.combust.mleap.runtime.serialization.json

import ml.combust.mleap.runtime.types.{BooleanType, DataType, LongType, _}
import ml.combust.mleap.runtime.{Dataset, LocalDataset, Row}
import spray.json.DefaultJsonProtocol._
import spray.json._

/**
  * Created by hollinwilkins on 9/10/16.
  */
object DatasetFormat {
  def listSerializer(lt: ListType): JsonFormat[_] = immSeqFormat(serializer(lt.base))
  def tensorSerializer(tt: TensorType): JsonFormat[_] = {
    assert(tt.dimensions.length == 1, s"unsupported tensor type: $tt")

    tt.base match {
      case DoubleType => JsonSupport.mleapVectorFormat
      case StringType => immSeqFormat[String]
      case _ => serializationError(s"unsupported tensor type: $tt")
    }
  }

  def serializer(tpe: DataType): JsonFormat[_] = tpe match {
    case StringType => StringJsonFormat
    case DoubleType => DoubleJsonFormat
    case BooleanType => BooleanJsonFormat
    case LongType => LongJsonFormat
    case IntegerType => IntJsonFormat
    case lt: ListType => listSerializer(lt)
    case tt: TensorType => tensorSerializer(tt)
    case ct: CustomType => ct.format
    case AnyType => serializationError("AnyType unsupported for serialization")
  }
}

case class DatasetFormat(schema: StructType) extends RootJsonFormat[Dataset] {
  val rowFormat: RowFormat = RowFormat(schema)

  override def write(obj: Dataset): JsValue = {
    val values = obj.toArray.map(rowFormat.write)
    JsArray(values: _*)
  }

  override def read(json: JsValue): Dataset = json match {
    case json: JsArray =>
      val rows = json.elements.map {
        case jsValues: JsArray =>
          val values = jsValues.elements.map(rowFormat.read)
          Row(values: _*)
        case _ => deserializationError("invalid dataset row")
      }

      LocalDataset(data = rows.toArray)
    case _ => deserializationError("invalid dataset")
  }
}
