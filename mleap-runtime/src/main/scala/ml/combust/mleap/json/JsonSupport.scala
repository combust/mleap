package ml.combust.mleap.json

import java.util.Base64

import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.{DefaultLeapFrame, LeapFrame, MleapContext}
import ml.combust.mleap.runtime.types._
import ml.combust.mleap.tensor.ByteString
import spray.json.DefaultJsonProtocol._
import spray.json._
import spray.json.{JsValue, JsonFormat}

import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 8/23/16.
  */
trait JsonSupport extends ml.combust.mleap.tensor.JsonSupport {
  implicit val BundleByteStringFormat: JsonFormat[ByteString] = new JsonFormat[ByteString] {
    override def write(obj: ByteString) = {
      JsString(Base64.getEncoder.encodeToString(obj.bytes))
    }

    override def read(json: JsValue) = json match {
      case JsString(b64) => ByteString(Base64.getDecoder.decode(b64))
      case _ => deserializationError("invalid byte string")
    }
  }

  implicit val mleapListTypeWriterFormat: JsonWriter[ListType] = new JsonWriter[ListType] {
    override def write(obj: ListType): JsValue = {
      JsObject("type" -> JsString("list"),
        "base" -> obj.base.toJson)
    }
  }

  implicit def mleapListTypeReaderFormat(implicit context: MleapContext): JsonReader[ListType] = new JsonReader[ListType] {
    override def read(json: JsValue): ListType = {
      val obj = json.asJsObject("invalid list type")

      ListType(obj.fields("base").convertTo[DataType])
    }
  }

  implicit val mleapTensorTypeFormat: JsonFormat[TensorType] = lazyFormat(new JsonFormat[TensorType] {
    override def write(obj: TensorType): JsValue = {
      JsObject("type" -> JsString("tensor"),
        "base" -> mleapBasicTypeFormat.write(obj.base))
    }

    override def read(json: JsValue): TensorType = {
      val obj = json.asJsObject("invalid tensor type")

      TensorType(base = mleapBasicTypeFormat.read(obj.fields("base")))
    }
  })

  val mleapBasicTypeFormat: JsonFormat[BasicType] = lazyFormat(new JsonFormat[BasicType] {
    def writeMaybeNullable(base: JsString, isNullable: Boolean): JsValue = {
      if(isNullable) {
        JsObject("type" -> JsString("basic"),
          "base" -> base,
          "isNullable" -> JsBoolean(true))
      } else { base }
    }

    def readNullable(json: JsObject): BasicType = {
      json.getFields("base", "isNullable") match {
        case Seq(JsString(base), JsBoolean(isNullable)) => forName(base, isNullable)
        case _ => deserializationError("invalid basic type format")
      }
    }

    def forName(name: String, isNullable: Boolean = false): BasicType = name match {
      case "boolean" => BooleanType(isNullable)
      case "string" => StringType(isNullable)
      case "byte" => ByteType(isNullable)
      case "short" => ShortType(isNullable)
      case "integer" => IntegerType(isNullable)
      case "long" => LongType(isNullable)
      case "float" => FloatType(isNullable)
      case "double" => DoubleType(isNullable)
      case "byte_string" => ByteStringType(isNullable)
    }

    override def write(obj: BasicType): JsValue = obj match {
      case BooleanType(isNullable) => writeMaybeNullable(JsString("boolean"), isNullable)
      case StringType(isNullable) => writeMaybeNullable(JsString("string"), isNullable)
      case ByteType(isNullable) => writeMaybeNullable(JsString("byte"), isNullable)
      case ShortType(isNullable) => writeMaybeNullable(JsString("short"), isNullable)
      case IntegerType(isNullable) => writeMaybeNullable(JsString("integer"), isNullable)
      case LongType(isNullable) => writeMaybeNullable(JsString("long"), isNullable)
      case FloatType(isNullable) => writeMaybeNullable(JsString("float"), isNullable)
      case DoubleType(isNullable) => writeMaybeNullable(JsString("double"), isNullable)
      case ByteStringType(isNullable) => writeMaybeNullable(JsString("byte_string"), isNullable)
    }

    override def read(json: JsValue): BasicType = json match {
      case JsString(name) => forName(name)
      case json: JsObject => readNullable(json)
      case _ => throw new Error("Invalid basic type") // TODO: better error
    }
  })

  implicit val mleapDataTypeWriterFormat: JsonWriter[DataType] = new JsonWriter[DataType] {
    override def write(obj: DataType): JsValue = obj match {
      case bt: BasicType => mleapBasicTypeFormat.write(bt)
      case lt: ListType => mleapListTypeWriterFormat.write(lt)
      case tt: TensorType => mleapTensorTypeFormat.write(tt)
      case _ => serializationError(s"$obj not supported for JSON serialization")
    }
  }

  implicit def mleapDataTypeReaderFormat(implicit context: MleapContext): JsonReader[DataType] = new JsonReader[DataType] {
    override def read(json: JsValue): DataType = json match {
      case obj: JsString => mleapBasicTypeFormat.read(obj)
      case obj: JsObject =>
        obj.fields.get("type") match {
          case Some(JsString("basic")) => mleapBasicTypeFormat.read(obj)
          case Some(JsString("list")) => mleapListTypeReaderFormat.read(obj)
          case Some(JsString("tensor")) => mleapTensorTypeFormat.read(obj)
          case _ => deserializationError(s"invalid data type: ${obj.fields.get("type")}")
        }
      case _ => throw new Error("Invalid data type") // TODO: better error
    }
  }

  implicit val mleapStructFieldWriterFormat: JsonWriter[StructField] = new JsonWriter[StructField] {
    override def write(obj: StructField): JsValue = {
      JsObject("name" -> JsString(obj.name),
        "type" -> obj.dataType.toJson)
    }
  }

  implicit def mleapStructFieldReaderFormat(implicit context: MleapContext): JsonReader[StructField] = new JsonReader[StructField] {
    override def read(json: JsValue): StructField = json match {
      case obj: JsObject =>
        val name = StringJsonFormat.read(obj.fields("name"))
        val dataType = mleapDataTypeReaderFormat.read(obj.fields("type"))

        StructField(name = name, dataType = dataType)
      case _ => throw new Error("Invalid StructField") // TODO: better error
    }
  }

  implicit val mleapStructTypeWriterFormat: JsonWriter[StructType] = new JsonWriter[StructType] {
    implicit val writer = lift(mleapStructFieldWriterFormat)

    override def write(obj: StructType): JsValue = {
      JsObject("fields" -> obj.fields.toJson)
    }
  }

  implicit def mleapStructTypeReaderFormat(implicit context: MleapContext): RootJsonReader[StructType] = new RootJsonReader[StructType] {
    implicit val reader = lift(mleapStructFieldReaderFormat)

    override def read(json: JsValue): StructType = json match {
      case json: JsObject =>
        json.fields.get("fields") match {
          case Some(fields: JsArray) =>
            StructType(fields.convertTo[Seq[StructField]]).get
          case _ => throw new Error("Invalid StructType") // TODO: better error
        }
      case _ => throw new Error("Invalid StructType") // TODO: better error
    }
  }

  implicit def mleapLeapFrameWriterFormat[LF <: LeapFrame[LF]]: JsonWriter[LF] = new JsonWriter[LF] {
    override def write(obj: LF): JsValue = {
      val formatter = DatasetFormat(obj.schema)
      val rows = formatter.write(obj.dataset)
      JsObject(("schema", obj.schema.toJson), ("rows", rows))
    }
  }

  implicit def mleapDefaultLeapFrameReaderFormat(implicit context: MleapContext): RootJsonReader[DefaultLeapFrame] = new RootJsonReader[DefaultLeapFrame] {
    override def read(json: JsValue): DefaultLeapFrame = {
      val obj = json.asJsObject("invalid LeapFrame")

      val schema = obj.fields("schema").convertTo[StructType]
      val formatter = DatasetFormat(schema)
      val rows = formatter.read(obj.fields("rows"))

      DefaultLeapFrame(schema, rows)
    }
  }
}
object JsonSupport extends JsonSupport
