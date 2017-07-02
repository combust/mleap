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

  def listSerializer(lt: ListType): JsonFormat[_] = seqFormat(serializer(lt.base))

  def tensorSerializer(tt: TensorType): JsonFormat[_] = {
    val isNullable = tt.isNullable

    tt.base match {
      case BooleanType(false) => maybeNullableFormat(mleapTensorFormat[Boolean], isNullable)
      case ByteType(false) => maybeNullableFormat(mleapTensorFormat[Byte], isNullable)
      case ShortType(false) => maybeNullableFormat(mleapTensorFormat[Short], isNullable)
      case IntegerType(false) => maybeNullableFormat(mleapTensorFormat[Int], isNullable)
      case LongType(false) => maybeNullableFormat(mleapTensorFormat[Long], isNullable)
      case FloatType(false) => maybeNullableFormat(mleapTensorFormat[Float], isNullable)
      case DoubleType(false) => maybeNullableFormat(mleapTensorFormat[Double], isNullable)
      case StringType(false) => maybeNullableFormat(mleapTensorFormat[String], isNullable)
      case ByteStringType(false) => maybeNullableFormat(mleapTensorFormat[ByteString], isNullable)
      case _ => serializationError(s"invalid tensor base type: ${tt.base}")
    }
  }

  def serializer(tpe: DataType): JsonFormat[_] = tpe match {
    case BooleanType(isNullable) => maybeNullableFormat(BooleanJsonFormat, isNullable)
    case StringType(isNullable) => maybeNullableFormat(StringJsonFormat, isNullable)
    case ByteType(isNullable) => maybeNullableFormat(ByteJsonFormat, isNullable)
    case ShortType(isNullable) => maybeNullableFormat(ShortJsonFormat, isNullable)
    case IntegerType(isNullable) => maybeNullableFormat(IntJsonFormat, isNullable)
    case LongType(isNullable) => maybeNullableFormat(LongJsonFormat, isNullable)
    case FloatType(isNullable) => maybeNullableFormat(FloatJsonFormat, isNullable)
    case DoubleType(isNullable) => maybeNullableFormat(DoubleJsonFormat, isNullable)
    case ByteStringType(isNullable) => maybeNullableFormat(BundleByteStringFormat, isNullable)
    case lt: ListType => listSerializer(lt)
    case tt: TensorType => tensorSerializer(tt)
    case AnyType(_) => serializationError("AnyType unsupported for serialization")
    case _: TupleType => serializationError("DataTypeSeq only used for UDFs with multiple outputs")
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
