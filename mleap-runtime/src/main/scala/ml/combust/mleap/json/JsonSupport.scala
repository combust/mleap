package ml.combust.mleap.json

import java.util.Base64

import ml.combust.mleap.core.frame.{DefaultLeapFrame, LeapFrame, Row}
import ml.combust.mleap.core.types._
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

  implicit val MleapBasicTypeFormat: JsonFormat[BasicType] = new JsonFormat[BasicType] {
    override def write(obj: BasicType): JsValue = {
      val name = obj match {
        case BasicType.Boolean => "boolean"
        case BasicType.Byte => "byte"
        case BasicType.Short => "short"
        case BasicType.Int => "int"
        case BasicType.Long => "long"
        case BasicType.Float => "float"
        case BasicType.Double => "double"
        case BasicType.String => "string"
        case BasicType.ByteString => "byte_string"
      }

      JsString(name)
    }

    override def read(json: JsValue): BasicType = json match {
      case JsString(name) => name match {
        case "boolean" => BasicType.Boolean
        case "byte" => BasicType.Byte
        case "short" => BasicType.Short
        case "int" => BasicType.Int
        case "long" => BasicType.Long
        case "float" => BasicType.Float
        case "double" => BasicType.Double
        case "string" => BasicType.String
        case "byte_string" => BasicType.ByteString
      }
      case _ => deserializationError(s"invalid basic type: $json")
    }
  }

  implicit val MleapListTypeFormat: JsonFormat[ListType] = new JsonFormat[ListType] {
    override def write(obj: ListType): JsObject = {
      var fields = Seq("type" -> JsString("list"),
        "base" -> obj.base.toJson)

      if(!obj.isNullable) {
        fields :+= "isNullable" -> JsBoolean(false)
      }

      JsObject(fields: _*)
    }

    override def read(json: JsValue): ListType = {
      val obj = json.asJsObject("invalid list type")
      val isNullable = obj.fields.get("isNullable").forall(_.convertTo[Boolean])

      ListType(obj.fields("base").convertTo[BasicType], isNullable)
    }
  }

  implicit val MleapTensorTypeFormat: JsonFormat[TensorType] = lazyFormat(new JsonFormat[TensorType] {
    override def write(obj: TensorType): JsValue = {
      var map = Map("type" -> JsString("tensor"),
        "base" -> MleapBasicTypeFormat.write(obj.base))

      if(!obj.isNullable) {
        map += "isNullable" -> JsBoolean(false)
      }

      obj.dimensions.foreach {
        dims => map = map + ("dimensions" -> dims.toJson)
      }

      JsObject(map)
    }

    override def read(json: JsValue): TensorType = {
      val obj = json.asJsObject("invalid tensor type")
      val isNullable = obj.fields.get("isNullable").forall(_.convertTo[Boolean])

      val dimensions = obj.fields.get("dimensions").map {
        dims => dims.convertTo[Seq[Int]]
      }
      TensorType(base = obj.fields("base").convertTo[BasicType],
        dimensions = dimensions,
        isNullable = isNullable)
    }
  })

  val MleapScalarTypeFormat: JsonFormat[ScalarType] = lazyFormat(new JsonFormat[ScalarType] {
    def writeMaybeNullable(base: JsString, isNullable: Boolean): JsValue = {
      if(isNullable) {
        base
      } else {
        JsObject("type" -> JsString("basic"),
          "base" -> base,
          "isNullable" -> JsBoolean(false))
      }
    }

    def readNullable(json: JsObject): ScalarType = {
      json.getFields("base", "isNullable") match {
        case Seq(JsString(base), JsBoolean(isNullable)) => forName(base).setNullable(isNullable)
        case _ => deserializationError("invalid basic type format")
      }
    }

    def forName(name: String): ScalarType = name match {
      case "boolean" => ScalarType.Boolean
      case "byte" => ScalarType.Byte
      case "short" => ScalarType.Short
      case "integer" => ScalarType.Int
      case "long" => ScalarType.Long
      case "float" => ScalarType.Float
      case "double" => ScalarType.Double
      case "string" => ScalarType.String
      case "byte_string" => ScalarType.ByteString
      case _ => deserializationError(s"invalid basic type name $name")
    }

    override def write(obj: ScalarType): JsValue = obj.base match {
      case BasicType.Boolean => writeMaybeNullable(JsString("boolean"), obj.isNullable)
      case BasicType.Byte => writeMaybeNullable(JsString("byte"), obj.isNullable)
      case BasicType.Short => writeMaybeNullable(JsString("short"), obj.isNullable)
      case BasicType.Int => writeMaybeNullable(JsString("integer"), obj.isNullable)
      case BasicType.Long => writeMaybeNullable(JsString("long"), obj.isNullable)
      case BasicType.Float => writeMaybeNullable(JsString("float"), obj.isNullable)
      case BasicType.Double => writeMaybeNullable(JsString("double"), obj.isNullable)
      case BasicType.String => writeMaybeNullable(JsString("string"), obj.isNullable)
      case BasicType.ByteString => writeMaybeNullable(JsString("byte_string"), obj.isNullable)
    }

    override def read(json: JsValue): ScalarType = json match {
      case JsString(name) => forName(name)
      case json: JsObject => readNullable(json)
      case _ => throw new Error("Invalid basic type") // TODO: better error
    }
  })

  implicit val MleapDataTypeFormat: JsonFormat[DataType] = new JsonFormat[DataType] {
    override def write(obj: DataType): JsValue = obj match {
      case st: ScalarType => MleapScalarTypeFormat.write(st)
      case lt: ListType => MleapListTypeFormat.write(lt)
      case tt: TensorType => MleapTensorTypeFormat.write(tt)
      case _ => serializationError(s"$obj not supported for JSON serialization")
    }

    override def read(json: JsValue): DataType = json match {
      case obj: JsString => MleapScalarTypeFormat.read(obj)
      case obj: JsObject =>
        obj.fields.get("type") match {
          case Some(JsString("basic")) => MleapScalarTypeFormat.read(obj)
          case Some(JsString("list")) => MleapListTypeFormat.read(obj)
          case Some(JsString("tensor")) => MleapTensorTypeFormat.read(obj)
          case _ => deserializationError(s"invalid data type: ${obj.fields.get("type")}")
        }
      case _ => deserializationError(s"Invalid data type $json")
    }
  }

  implicit val MleapStructFieldFormat: JsonFormat[StructField] = new JsonFormat[StructField] {
    override def write(obj: StructField): JsValue = {
      JsObject("name" -> JsString(obj.name),
        "type" -> obj.dataType.toJson)
    }

    override def read(json: JsValue): StructField = json match {
      case obj: JsObject =>
        val name = StringJsonFormat.read(obj.fields("name"))
        val dataType = MleapDataTypeFormat.read(obj.fields("type"))

        StructField(name = name, dataType = dataType)
      case _ => throw new Error("Invalid StructField") // TODO: better error
    }
  }

  implicit val MleapStructTypeFormat: RootJsonFormat[StructType] = new RootJsonFormat[StructType] {
    override def write(obj: StructType): JsValue = {
      JsObject("fields" -> obj.fields.toJson)
    }

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

  implicit def MleapLeapFrameWriterFormat[LF <: LeapFrame[LF]]: JsonWriter[LF] = new JsonWriter[LF] {
    override def write(obj: LF): JsValue = {
      implicit val formatter = RowFormat(obj.schema)
      val rows = obj.collect().toJson
      JsObject(("schema", obj.schema.toJson), ("rows", rows))
    }
  }

  implicit val MleapDefaultLeapFrameReaderFormat: RootJsonReader[DefaultLeapFrame] = new RootJsonReader[DefaultLeapFrame] {
    override def read(json: JsValue): DefaultLeapFrame = {
      val obj = json.asJsObject("invalid LeapFrame")

      val schema = obj.fields("schema").convertTo[StructType]
      implicit val formatter = RowFormat(schema)
      val rows = obj.fields("rows").convertTo[Seq[Row]]

      DefaultLeapFrame(schema, rows)
    }
  }
}
object JsonSupport extends JsonSupport
