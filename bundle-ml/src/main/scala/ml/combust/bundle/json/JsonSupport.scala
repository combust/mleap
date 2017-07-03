package ml.combust.bundle.json

import java.util.{Base64, UUID}

import com.google.protobuf.ByteString
import ml.bundle._
import spray.json.DefaultJsonProtocol._
import spray.json._

/** spray.json.JsonFormat formats for protobuf objects in Bundle.ML.
  *
  * Includes many spray.json.JsonFormat format implicits as well as several
  * spray.json.RootJsonFormat format implicits for top-level JSON objects.
  *
  * There are no members that need to be overriden if using this trait as a mixin.
  */
trait JsonSupport {
  implicit object UUIDFormat extends JsonFormat[UUID] {
    override def read(json: JsValue): UUID = json match {
      case JsString(value) => UUID.fromString(value)
      case _ => throw new IllegalArgumentException("invalid UUID")
    }

    override def write(obj: UUID): JsValue = JsString(obj.toString)
  }

  implicit val bundleSocketFormat: JsonFormat[Socket] = jsonFormat2(Socket.apply)

  implicit val bundleNodeShapeFormat: JsonFormat[NodeShape] = new JsonFormat[NodeShape] {
    override def write(obj: NodeShape): JsValue = {
      JsObject("inputs" -> obj.inputs.toJson,
        "outputs" -> obj.outputs.toJson)
    }

    override def read(json: JsValue): NodeShape = json match {
      case json: JsObject =>
        val inputs = json.fields("inputs").convertTo[Seq[Socket]]
        val outputs = json.fields("outputs").convertTo[Seq[Socket]]
        NodeShape(inputs, outputs)
      case _ => deserializationError("invalid shape")
    }
  }

  implicit val bundleBasicTypeFormat: JsonFormat[BasicType] = new JsonFormat[BasicType] {
    override def read(json: JsValue): BasicType = json match {
      case JsString("boolean") => BasicType.BOOLEAN
      case JsString("byte") => BasicType.BYTE
      case JsString("short") => BasicType.SHORT
      case JsString("int") => BasicType.INT
      case JsString("long") => BasicType.LONG
      case JsString("float") => BasicType.FLOAT
      case JsString("double") => BasicType.DOUBLE
      case JsString("string") => BasicType.STRING
      case JsString("byte_string") => BasicType.BYTE_STRING
      case _ => deserializationError("invalid basic type")
    }

    override def write(obj: BasicType): JsValue = obj match {
      case BasicType.BOOLEAN => JsString("boolean")
      case BasicType.BYTE => JsString("byte")
      case BasicType.SHORT => JsString("short")
      case BasicType.INT => JsString("int")
      case BasicType.LONG => JsString("long")
      case BasicType.FLOAT => JsString("float")
      case BasicType.DOUBLE => JsString("double")
      case BasicType.STRING => JsString("string")
      case BasicType.BYTE_STRING => JsString("byte_string")
      case _ => serializationError("invalid basic type")
    }
  }

  implicit val BundleByteStringFormat: JsonFormat[ByteString] = new JsonFormat[ByteString] {
    override def write(obj: ByteString) = {
      JsString(Base64.getEncoder.encodeToString(obj.toByteArray))
    }

    override def read(json: JsValue) = json match {
      case JsString(b64) => ByteString.copyFrom(Base64.getDecoder.decode(b64))
      case _ => deserializationError("invalid byte string")
    }
  }

  implicit val bundleTensorDimensionFormat: JsonFormat[TensorDimension] = jsonFormat2(TensorDimension.apply)
  implicit val bundleTensorShapeFormat: JsonFormat[TensorShape] = jsonFormat1(TensorShape.apply)
  implicit val bundleDataShapeTypeFormat: JsonFormat[DataShapeType] = new JsonFormat[DataShapeType] {
    override def write(obj: DataShapeType) = obj match {
      case DataShapeType.SCALAR => JsString("scalar")
      case DataShapeType.LIST => JsString("list")
      case DataShapeType.TENSOR => JsString("tensor")
      case _ => serializationError(s"invalid data shape type $obj")
    }

    override def read(json: JsValue) = json match {
      case JsString("scalar") => DataShapeType.SCALAR
      case JsString("list") => DataShapeType.LIST
      case JsString("tensor") => DataShapeType.TENSOR
      case _ => deserializationError(s"invalid data shape type $json")
    }
  }
  implicit val bundleDataShapeFormat: JsonFormat[DataShape] = jsonFormat3(DataShape.apply)

  implicit val bundleScalarFormat: JsonFormat[Scalar] = jsonFormat11(Scalar.apply)
  implicit val bundleTensorFormat: JsonFormat[Tensor] = jsonFormat3(Tensor.apply)
  implicit val bundleListFormat: JsonFormat[List] = jsonFormat11(List.apply)

  implicit val bundleValueFormat: JsonFormat[Value] = new JsonFormat[Value] {
    override def write(obj: Value) = {
      if(obj.v.isS) {
        obj.getS.toJson
      } else if(obj.v.isL) {
        val fields = obj.getL.toJson.asJsObject.fields + ("type" -> JsString("list"))
        JsObject(fields)
      } else if(obj.v.isT) {
        val fields = obj.getT.toJson.asJsObject.fields + ("type" -> JsString("tensor"))
        JsObject(fields)
      } else {
        serializationError(s"invalid value $obj")
      }
    }

    override def read(json: JsValue) = json match {
      case json: JsObject =>
        json.fields.get("type") match {
          case None => Value(Value.V.S(json.convertTo[Scalar]))
          case Some(JsString("list")) => Value(Value.V.L(json.convertTo[List]))
          case Some(JsString("tensor")) => Value(Value.V.T(json.convertTo[Tensor]))
          case Some(j) => deserializationError(s"invalid type $j")
        }
      case _ => deserializationError(s"invalid value $json")
    }
  }

  implicit val bundleFormatFormat: JsonFormat[Format] = new JsonFormat[Format] {
    override def write(obj: Format): JsValue = obj match {
      case Format.PROTOBUF => JsString("protobuf")
      case Format.JSON => JsString("json")
      case _ => serializationError("invalid format")
    }

    override def read(json: JsValue): Format = json match {
      case JsString("json") => Format.JSON
      case JsString("protobuf") => Format.PROTOBUF
      case _ => deserializationError("invalid format")
    }
  }

  implicit val bundleAttributesFormat: JsonFormat[Attributes] = new JsonFormat[Attributes] {
    override def write(obj: Attributes) = obj.list.toJson

    override def read(json: JsValue) = Attributes(json.convertTo[Map[String, Value]])
  }

  implicit val bundleNodeFormat: RootJsonFormat[Node] = jsonFormat2(Node.apply)
  implicit val bundleModelFormat: RootJsonFormat[Model] = jsonFormat2(Model.apply)
  implicit val bundleBundleInfoFormat: RootJsonFormat[Bundle] = jsonFormat5(Bundle.apply)
}
object JsonSupport extends JsonSupport