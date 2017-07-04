package ml.combust.mleap.json

import ml.combust.mleap.runtime.{Dataset, LocalDataset}
import spray.json.DefaultJsonProtocol._
import spray.json._
import JsonSupport.{mleapTensorFormat, BundleByteStringFormat}
import ml.combust.mleap.core.types._
import ml.combust.mleap.tensor.ByteString

/**
  * Created by hollinwilkins on 9/10/16.
  */
object DatasetFormat {
  def maybeNullableFormat[T](base: JsonFormat[T],
                             isNullable: Boolean): JsonFormat[_] = {
    if(isNullable) {
      optionFormat(base)
    } else {
      base
    }
  }

  def listSerializer(lt: ListType): JsonFormat[_] = seqFormat(basicSerializer(lt.base, isNullable = false))

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
  }

  def scalarSerializer(st: ScalarType): JsonFormat[_] = basicSerializer(st.base, st.isNullable)

  def serializer(dt: DataType): JsonFormat[_] = dt match {
    case st: ScalarType => scalarSerializer(st)
    case lt: ListType => listSerializer(lt)
    case tt: TensorType => tensorSerializer(tt)
  }
}

case class DatasetFormat(schema: StructType) extends RootJsonFormat[Dataset] {
  val rowFormat: RowFormat = RowFormat(schema)

  override def write(obj: Dataset): JsValue = {
    val values = obj.toArray.map(rowFormat.write)
    JsArray(values: _*)
  }

  override def read(json: JsValue): Dataset = {
    json match {
      case json: JsArray =>
        val rows = json.elements.map(rowFormat.read)
        LocalDataset(data = rows)
      case _ => deserializationError("invalid dataset")
    }
  }
}
