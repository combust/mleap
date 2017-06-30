package ml.combust.bundle.json

import java.util.{Base64, UUID}

import ml.bundle.BasicType.BasicType
import ml.bundle.DataShape.{BaseDataShape, DataShape}
import ml.combust.bundle.dsl._
import ml.combust.bundle.serializer.SerializationFormat
import ml.bundle.DataType.DataType
import ml.bundle.Socket.Socket
import ml.combust.bundle.ByteString
import ml.combust.mleap.tensor.JsonSupport._
import ml.combust.mleap.tensor.Tensor
import spray.json.DefaultJsonProtocol._
import spray.json._

/** Low priority implicit spray.json.JsonFormat formats for protobuf objects in Bundle.ML.
  *
  * Includes many spray.json.JsonFormat format implicits as well as several
  * spray.json.RootJsonFormat format implicits for top-level JSON objects.
  *
  * There are no members that need overrides if using this trait as a mixin.
  */
trait JsonSupportLowPriority {
  implicit object UUIDFormat extends JsonFormat[UUID] {
    override def read(json: JsValue): UUID = json match {
      case JsString(value) => UUID.fromString(value)
      case _ => throw new IllegalArgumentException("invalid UUID")
    }

    override def write(obj: UUID): JsValue = JsString(obj.toString)
  }

  implicit val bundleSocketFormat: JsonFormat[Socket] = jsonFormat2(Socket.apply)

  implicit val bundleShapeFormat: JsonFormat[Shape] = new JsonFormat[Shape] {
    override def write(obj: Shape): JsValue = {
      JsObject("inputs" -> obj.inputs.toJson,
        "outputs" -> obj.outputs.toJson)
    }

    override def read(json: JsValue): Shape = json match {
      case json: JsObject =>
        val inputs = json.fields("inputs").convertTo[Seq[Socket]]
        val outputs = json.fields("outputs").convertTo[Seq[Socket]]
        Shape(inputs, outputs)
      case _ => deserializationError("invalid shape")
    }
  }

  implicit val bundleByteStringFormat: JsonFormat[ByteString] = new JsonFormat[ByteString] {
    override def write(obj: ByteString): JsValue = {
      JsString(Base64.getEncoder.encodeToString(obj.bytes))
    }

    override def read(json: JsValue): ByteString = json match {
      case JsString(str) => ByteString(Base64.getDecoder.decode(str))
      case _ => deserializationError("invalid byte string format")
    }
  }

  implicit val bundleBasicTypeFormat: JsonFormat[BasicType] = new JsonFormat[BasicType] {
    override def read(json: JsValue): BasicType = json match {
      case JsString("boolean") => BasicType.BOOLEAN
      case JsString("string") => BasicType.STRING
      case JsString("byte") => BasicType.BYTE
      case JsString("short") => BasicType.SHORT
      case JsString("int") => BasicType.INT
      case JsString("long") => BasicType.LONG
      case JsString("float") => BasicType.FLOAT
      case JsString("double") => BasicType.DOUBLE
      case JsString("byte_string") => BasicType.BYTE_STRING
      case JsString("data_type") => BasicType.DATA_TYPE
      case JsString("data_shape") => BasicType.DATA_SHAPE
      case JsString("basic_type") => BasicType.BASIC_TYPE
      case _ => deserializationError("invalid basic type")
    }

    override def write(obj: BasicType): JsValue = obj match {
      case BasicType.BOOLEAN => JsString("boolean")
      case BasicType.STRING => JsString("string")
      case BasicType.BYTE => JsString("byte")
      case BasicType.SHORT => JsString("short")
      case BasicType.INT => JsString("int")
      case BasicType.LONG => JsString("long")
      case BasicType.FLOAT => JsString("float")
      case BasicType.DOUBLE => JsString("double")
      case BasicType.BYTE_STRING => JsString("byte_string")
      case BasicType.DATA_TYPE => JsString("data_type")
      case BasicType.DATA_SHAPE => JsString("data_shape")
      case BasicType.BASIC_TYPE => JsString("basic_type")
      case _ => serializationError("invalid basic type")
    }
  }

  implicit val bundleDataShapeFormat: JsonFormat[DataShape] = new JsonFormat[DataShape] {
    override def read(json: JsValue): DataShape = json match {
      case JsString("scalar") => DataShape(base = BaseDataShape.SCALAR)
      case JsString("list") => DataShape(base = BaseDataShape.LIST)
      case arr: JsArray =>
        val dimensions = arr.convertTo[Seq[Int]]
        DataShape(base = BaseDataShape.TENSOR, dimensions = dimensions)
      case _ => deserializationError("invalid data shape")
    }

    override def write(obj: DataShape): JsValue = {
      obj.base match {
        case BaseDataShape.SCALAR => JsString("scalar")
        case BaseDataShape.LIST => JsString("list")
        case BaseDataShape.TENSOR => JsArray(obj.dimensions.map(_.toJson): _*)
        case _ => serializationError("invalid data shape")
      }
    }
  }

  implicit val bundleDataTypeFormat: JsonFormat[DataType] = new JsonFormat[DataType] {
    override def read(json: JsValue): DataType = json match {
      case _: JsString =>
        DataType(base = bundleBasicTypeFormat.read(json),
          shape = Some(DataShape(BaseDataShape.SCALAR)))
      case obj: JsObject =>
        obj.fields("type") match {
          case JsString("list") =>
            DataType(base = bundleBasicTypeFormat.read(obj.fields("base")),
              shape = Some(DataShape(BaseDataShape.LIST)))
          case JsString("tensor") =>
            DataType(base = bundleBasicTypeFormat.read(obj.fields("base")),
              shape = Some(DataShape(BaseDataShape.TENSOR)))
          case _ => deserializationError("invalid data type")
        }
      case _ => deserializationError("invalid data type")
    }

    override def write(obj: DataType): JsValue = {
      if(obj.shape.get.base.isScalar) {
        bundleBasicTypeFormat.write(obj.base)
      } else if(obj.shape.get.base.isList) {
        JsObject(("type", JsString("list")), ("base", bundleBasicTypeFormat.write(obj.base)))
      } else if(obj.shape.get.base.isTensor) {
        JsObject(("type", JsString("tensor")), ("base", bundleBasicTypeFormat.write(obj.base)))
      } else {
        serializationError("invalid data type")
      }
    }
  }

  def bundleTensorValueFormat(base: BasicType): JsonFormat[Any] = new JsonFormat[Any] {
    override def read(json: JsValue): Any = {
      base match {
        case BasicType.BOOLEAN => json.convertTo[Tensor[Boolean]]
        case BasicType.STRING => json.convertTo[Tensor[String]]
        case BasicType.BYTE => json.convertTo[Tensor[Byte]]
        case BasicType.SHORT => json.convertTo[Tensor[Short]]
        case BasicType.INT => json.convertTo[Tensor[Int]]
        case BasicType.LONG => json.convertTo[Tensor[Long]]
        case BasicType.FLOAT => json.convertTo[Tensor[Float]]
        case BasicType.DOUBLE => json.convertTo[Tensor[Double]]
        case _ => deserializationError(s"unsupported tensor $base")
      }
    }

    override def write(obj: Any): JsValue = {
      base match {
        case BasicType.BOOLEAN => obj.asInstanceOf[Tensor[Boolean]].toJson
        case BasicType.STRING => obj.asInstanceOf[Tensor[String]].toJson
        case BasicType.BYTE => obj.asInstanceOf[Tensor[Byte]].toJson
        case BasicType.SHORT => obj.asInstanceOf[Tensor[Short]].toJson
        case BasicType.INT => obj.asInstanceOf[Tensor[Int]].toJson
        case BasicType.LONG => obj.asInstanceOf[Tensor[Long]].toJson
        case BasicType.FLOAT => obj.asInstanceOf[Tensor[Float]].toJson
        case BasicType.DOUBLE => obj.asInstanceOf[Tensor[Double]].toJson
        case _ => serializationError(s"unsupported tensor $base")
      }
    }
  }

  def bundleListValueFormat(base: BasicType): JsonFormat[Seq[Any]] = new JsonFormat[Seq[Any]] {
    override def write(obj: Seq[Any]): JsValue = {
      base match {
        case BasicType.BOOLEAN => obj.asInstanceOf[Seq[Boolean]].toJson
        case BasicType.STRING => obj.asInstanceOf[Seq[String]].toJson
        case BasicType.BYTE => obj.asInstanceOf[Seq[Byte]].toJson
        case BasicType.SHORT => obj.asInstanceOf[Seq[Short]].toJson
        case BasicType.INT => obj.asInstanceOf[Seq[Int]].toJson
        case BasicType.LONG => obj.asInstanceOf[Seq[Long]].toJson
        case BasicType.FLOAT => obj.asInstanceOf[Seq[Float]].toJson
        case BasicType.DOUBLE => obj.asInstanceOf[Seq[Double]].toJson
        case BasicType.BYTE_STRING => obj.asInstanceOf[Seq[ByteString]].toJson
        case BasicType.DATA_TYPE => obj.asInstanceOf[Seq[DataType]].toJson
        case BasicType.DATA_SHAPE => obj.asInstanceOf[Seq[DataShape]].toJson
        case BasicType.BASIC_TYPE => obj.asInstanceOf[Seq[BasicType]].toJson
        case _ => serializationError(s"invalid basic type $base")
      }
    }

    override def read(json: JsValue): Seq[Any] = {
      base match {
        case BasicType.BOOLEAN => json.convertTo[Seq[Boolean]]
        case BasicType.STRING => json.convertTo[Seq[String]]
        case BasicType.BYTE => json.convertTo[Seq[Byte]]
        case BasicType.SHORT => json.convertTo[Seq[Short]]
        case BasicType.INT => json.convertTo[Seq[Int]]
        case BasicType.LONG => json.convertTo[Seq[Long]]
        case BasicType.FLOAT => json.convertTo[Seq[Float]]
        case BasicType.DOUBLE => json.convertTo[Seq[Double]]
        case BasicType.BYTE_STRING => json.convertTo[Seq[ByteString]]
        case BasicType.DATA_TYPE => json.convertTo[Seq[DataType]]
        case BasicType.DATA_SHAPE => json.convertTo[Seq[DataShape]]
        case BasicType.BASIC_TYPE => json.convertTo[Seq[BasicType]]
        case _ => deserializationError(s"invalid basic type $base")
      }
    }
  }

  def bundleValueFormat(dt: DataType): JsonFormat[Any] = new JsonFormat[Any] {
    override def write(obj: Any): JsValue = {
      if(dt.shape.get.base.isList) {
        bundleListValueFormat(dt.base).write(obj.asInstanceOf[Seq[Any]])
      } else if(dt.shape.get.base.isTensor) {
        bundleTensorValueFormat(dt.base).write(obj)
      } else if(dt.shape.get.base.isScalar) {
        dt.base match {
          case BasicType.BOOLEAN => BooleanJsonFormat.write(obj.asInstanceOf[Boolean])
          case BasicType.STRING => StringJsonFormat.write(obj.asInstanceOf[String])
          case BasicType.BYTE => ByteJsonFormat.write(obj.asInstanceOf[Byte])
          case BasicType.SHORT => ShortJsonFormat.write(obj.asInstanceOf[Short])
          case BasicType.INT => IntJsonFormat.write(obj.asInstanceOf[Int])
          case BasicType.LONG => LongJsonFormat.write(obj.asInstanceOf[Long])
          case BasicType.FLOAT => FloatJsonFormat.write(obj.asInstanceOf[Float])
          case BasicType.DOUBLE => DoubleJsonFormat.write(obj.asInstanceOf[Double])
          case BasicType.BYTE_STRING => bundleByteStringFormat.write(obj.asInstanceOf[ByteString])
          case BasicType.DATA_TYPE => bundleDataTypeFormat.write(obj.asInstanceOf[DataType])
          case BasicType.DATA_SHAPE => bundleDataShapeFormat.write(obj.asInstanceOf[DataShape])
          case BasicType.BASIC_TYPE => bundleBasicTypeFormat.write(obj.asInstanceOf[BasicType])
          case _ => serializationError(s"invalid basic type ${dt.base}")
        }
      } else { serializationError("unsupported data type") }
    }

    override def read(json: JsValue): Any = {
      if(dt.shape.get.base.isList) {
        bundleListValueFormat(dt.base).read(json)
      } else if(dt.shape.get.base.isTensor) {
        bundleTensorValueFormat(dt.base).read(json)
      } else if(dt.shape.get.base.isScalar) {
        dt.base match {
          case BasicType.BOOLEAN => BooleanJsonFormat.read(json)
          case BasicType.STRING => StringJsonFormat.read(json)
          case BasicType.BYTE => ByteJsonFormat.read(json)
          case BasicType.SHORT => ShortJsonFormat.read(json)
          case BasicType.INT => IntJsonFormat.read(json)
          case BasicType.LONG => LongJsonFormat.read(json)
          case BasicType.FLOAT => FloatJsonFormat.read(json)
          case BasicType.DOUBLE => DoubleJsonFormat.read(json)
          case BasicType.BYTE_STRING => bundleByteStringFormat.read(json)
          case BasicType.DATA_TYPE => bundleDataTypeFormat.read(json)
          case BasicType.DATA_SHAPE => bundleDataShapeFormat.read(json)
          case BasicType.BASIC_TYPE => bundleBasicTypeFormat.read(json)
          case _ => deserializationError(s"invalid basic type ${dt.base}")
        }
      } else { deserializationError("unsupported data type") }
    }
  }

  implicit val bundleAttributeFormat: JsonFormat[Attribute] = new JsonFormat[Attribute] {
    override def read(json: JsValue): Attribute = json match {
      case json: JsObject =>
        val dt = bundleDataTypeFormat.read(json.fields("type"))
        val value = bundleValueFormat(dt).read(json.fields("value"))

        Attribute(value = Value(dt, value))
      case _ => deserializationError("invalid basic type")
    }

    override def write(obj: Attribute): JsValue = {
      implicit val format = bundleValueFormat(obj.value.bundleDataType)
      JsObject(("type", obj.value.bundleDataType.toJson),
        ("value", format.write(obj.value.value)))
    }
  }

  implicit val bundleEmbeddedAttributeListFormat: JsonFormat[AttributeList] = new JsonFormat[AttributeList] {
    override def write(obj: AttributeList): JsValue = obj.lookup.toJson
    override def read(json: JsValue): AttributeList = AttributeList(json.convertTo[Map[String, Attribute]])
  }

  implicit val bundleFormatFormat: JsonFormat[SerializationFormat] = new JsonFormat[SerializationFormat] {
    override def write(obj: SerializationFormat): JsValue = obj match {
      case SerializationFormat.Mixed => JsString("mixed")
      case SerializationFormat.Protobuf => JsString("protobuf")
      case SerializationFormat.Json => JsString("json")
      case _ => serializationError("invalid format")
    }

    override def read(json: JsValue): SerializationFormat = json match {
      case JsString("mixed") => SerializationFormat.Mixed
      case JsString("json") => SerializationFormat.Json
      case JsString("protobuf") => SerializationFormat.Protobuf
      case _ => deserializationError("invalid format")
    }
  }

  implicit val bundleModelFormat: RootJsonFormat[Model] = jsonFormat2(Model.apply)
  implicit val bundleNodeFormat: RootJsonFormat[Node] = jsonFormat2(Node.apply)
  implicit val bundleBundleInfoFormat: RootJsonFormat[BundleInfo] = jsonFormat5(BundleInfo)
}

/** All spray.json.RootJsonFormat formats needed for Bundle.ML JSON serialization.
  *
  * The 4 spray.json.RootJsonFormat formats provided are:
  * <ul>
  *   <li>[[JsonSupport.bundleBundleInfoFormat]]</li>
  *   <li>[[JsonSupport.bundleNodeFormat]]</li>
  *   <li>[[JsonSupport.bundleModelFormat]]</li>
  *   <li>[[JsonSupport.bundleAttributeListFormat]]</li>
  * </ul>
  *
  * These are the only 4 implicit formats needed to serialize Bundle.ML models.
  */
trait JsonSupport extends JsonSupportLowPriority {
  implicit val bundleAttributeListFormat: RootJsonFormat[AttributeList] = new RootJsonFormat[AttributeList] {
    override def read(json: JsValue): AttributeList = json match {
      case json: JsObject =>
        AttributeList(json.fields("attributes").convertTo[Map[String, Attribute]])
      case _ => deserializationError("invalid attribute list")
    }

    override def write(obj: AttributeList): JsValue = JsObject("attributes" -> obj.lookup.toJson)
  }
}

object JsonSupport extends JsonSupport
