package ml.combust.bundle.json

import ml.bundle.BasicType.BasicType
import ml.combust.bundle.dsl._
import ml.combust.bundle.serializer.SerializationFormat
import ml.bundle.DataType.DataType
import ml.bundle.DataType.DataType.ListType
import ml.bundle.TensorType.TensorType
import ml.bundle.Socket.Socket
import ml.combust.bundle.HasBundleRegistry
import spray.json.DefaultJsonProtocol._
import spray.json._

/** Low priority implicit spray.json.JsonFormat formats for protobuf objects in Bundle.ML.
  *
  * Includes many spray.json.JsonFormat format implicits as well as several
  * spray.json.RootJsonFormat format implicits for top-level JSON objects.
  *
  * There are no members that need to be overriden if using this trait as a mixin.
  */
trait JsonSupportLowPriority {
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

  protected implicit val bundleBasicTypeFormat: JsonFormat[BasicType] = new JsonFormat[BasicType] {
    override def read(json: JsValue): BasicType = json match {
      case JsString("double") => BasicType.DOUBLE
      case JsString("string") => BasicType.STRING
      case JsString("boolean") => BasicType.BOOLEAN
      case JsString("long") => BasicType.LONG
      case _ => deserializationError("invalid basic type")
    }

    override def write(obj: BasicType): JsValue = obj match {
      case BasicType.DOUBLE => JsString("double")
      case BasicType.STRING => JsString("string")
      case BasicType.BOOLEAN => JsString("boolean")
      case BasicType.LONG => JsString("long")
      case _ => serializationError("invalid basic type")
    }
  }

  protected implicit val bundleTensorTypeFormat: JsonFormat[TensorType] = jsonFormat2(TensorType.apply)

  protected implicit val bundleDataTypeFormat: JsonFormat[DataType] = new JsonFormat[DataType] {
    override def read(json: JsValue): DataType = json match {
      case _: JsString => DataType(DataType.Underlying.Basic(bundleBasicTypeFormat.read(json)))
      case obj: JsObject =>
        obj.fields("type") match {
          case JsString("list") =>
            DataType(DataType.Underlying.List(DataType.ListType(Some(read(obj.fields("base"))))))
          case JsString("tensor") =>
            DataType(DataType.Underlying.Tensor(bundleTensorTypeFormat.read(obj.fields("tensor"))))
          case JsString("custom") =>
            DataType(DataType.Underlying.Custom(StringJsonFormat.read(obj.fields("name"))))
          case _ => deserializationError("invalid data type")
        }
      case _ => deserializationError("invalid data type")
    }

    override def write(obj: DataType): JsValue = {
      if(obj.underlying.isBasic) {
        bundleBasicTypeFormat.write(obj.getBasic)
      } else if(obj.underlying.isList) {
        JsObject(("type", JsString("list")), ("base", write(obj.getList.base.get)))
      } else if(obj.underlying.isTensor) {
        JsObject(("type", JsString("tensor")), ("tensor", bundleTensorTypeFormat.write(obj.getTensor)))
      } else if(obj.underlying.isCustom) {
        JsObject(("type", JsString("custom")), ("name", JsString(obj.getCustom)))
      } else {
        serializationError("invalid data type")
      }
    }
  }

  protected def bundleTensorFormat(tt: TensorType): JsonFormat[Any] = new JsonFormat[Any] {
    override def read(json: JsValue): Any = {
      tt.base match {
        case BasicType.DOUBLE => json.convertTo[Seq[Double]]
        case BasicType.STRING => json.convertTo[Seq[String]]
        case _ => deserializationError("unsupported tensor")
      }
    }

    override def write(obj: Any): JsValue = {
      tt.base match {
        case BasicType.DOUBLE => obj.asInstanceOf[Seq[Double]].toJson
        case BasicType.STRING => obj.asInstanceOf[Seq[String]].toJson
        case _ => serializationError("unsupported tensor")
      }
    }
  }

  protected def bundleListValueFormat(lt: ListType)
                                     (implicit hr: HasBundleRegistry): JsonFormat[Seq[Any]] = new JsonFormat[Seq[Any]] {
    val base = lt.base.get
    override def write(obj: Seq[Any]): JsValue = {
      if(base.underlying.isCustom) {
        val ct = hr.bundleRegistry.custom[Any](base.getCustom)
        val values = obj.map(ct.format.write)
        JsArray(values: _*)
      } else if(base.underlying.isTensor) {
        val format = bundleTensorFormat(base.getTensor)
        JsArray(obj.map(format.write): _*)
      } else if(base.underlying.isBasic) {
        base.getBasic match {
          case BasicType.DOUBLE => obj.asInstanceOf[Seq[Double]].toJson
          case BasicType.STRING => obj.asInstanceOf[Seq[String]].toJson
          case BasicType.BOOLEAN => obj.asInstanceOf[Seq[Boolean]].toJson
          case BasicType.LONG => obj.asInstanceOf[Seq[Long]].toJson
          case _ => serializationError("invalid basic type")
        }
      } else if(base.underlying.isList) {
        val format = bundleListValueFormat(base.getList)
        JsArray(obj.asInstanceOf[Seq[Seq[Any]]].map(format.write): _*)
      } else { serializationError("invalid list") }
    }

    override def read(json: JsValue): Seq[Any] = {
      if(base.underlying.isCustom) {
        val format = hr.bundleRegistry.custom[Any](base.getCustom).format
        json.convertTo[Seq[JsValue]].map(format.read)
      } else if(base.underlying.isTensor) {
        val format = bundleTensorFormat(base.getTensor)
        json.convertTo[Seq[JsValue]].map(format.read)
      } else if(base.underlying.isBasic) {
        base.getBasic match {
          case BasicType.DOUBLE => json.convertTo[Seq[Double]]
          case BasicType.STRING => json.convertTo[Seq[String]]
          case BasicType.BOOLEAN => json.convertTo[Seq[Boolean]]
          case BasicType.LONG => json.convertTo[Seq[Long]]
          case _ => deserializationError("invalid basic type")
        }
      } else if(base.underlying.isList) {
        val format = bundleListValueFormat(base.getList)
        json.convertTo[Seq[JsValue]].map(format.read)
      } else { deserializationError("invalid list") }
    }
  }

  protected def bundleValueFormat(dt: DataType)
                                 (implicit hr: HasBundleRegistry): JsonFormat[Any] = new JsonFormat[Any] {
    override def write(obj: Any): JsValue = {
      if(dt.underlying.isCustom) {
        val format = hr.bundleRegistry.custom[Any](dt.getCustom).format
        format.write(obj)
      } else if(dt.underlying.isList) {
        bundleListValueFormat(dt.getList).write(obj.asInstanceOf[Seq[Any]])
      } else if(dt.underlying.isTensor) {
        bundleTensorFormat(dt.getTensor).write(obj)
      } else if(dt.underlying.isBasic) {
        dt.getBasic match {
          case BasicType.DOUBLE => DoubleJsonFormat.write(obj.asInstanceOf[Double])
          case BasicType.STRING => StringJsonFormat.write(obj.asInstanceOf[String])
          case BasicType.BOOLEAN => BooleanJsonFormat.write(obj.asInstanceOf[Boolean])
          case BasicType.LONG => LongJsonFormat.write(obj.asInstanceOf[Long])
          case _ => serializationError("invalid basic type")
        }
      } else { serializationError("unsupported data type") }
    }

    override def read(json: JsValue): Any = {
      val v = if(dt.underlying.isCustom) {
        val format = hr.bundleRegistry.custom[Any](dt.getCustom).format
        format.read(json)
      } else if(dt.underlying.isList) {
        bundleListValueFormat(dt.getList).read(json)
      } else if(dt.underlying.isTensor) {
        bundleTensorFormat(dt.getTensor).read(json)
      } else if(dt.underlying.isBasic) {
        dt.getBasic match {
          case BasicType.DOUBLE => DoubleJsonFormat.read(json)
          case BasicType.STRING => StringJsonFormat.read(json)
          case BasicType.BOOLEAN => BooleanJsonFormat.read(json)
          case BasicType.LONG => LongJsonFormat.read(json)
          case _ => deserializationError("invalid basic type")
        }
      } else { deserializationError("unsupported data type") }

      v
    }
  }

  protected implicit def bundleAttributeFormat(implicit hr: HasBundleRegistry): JsonFormat[Attribute] = new JsonFormat[Attribute] {
    override def read(json: JsValue): Attribute = json match {
      case json: JsObject =>
        val name = StringJsonFormat.read(json.fields("name"))
        val dt = bundleDataTypeFormat.read(json.fields("type"))
        val value = bundleValueFormat(dt).read(json.fields("value"))

        Attribute(name = name, value = Value(dt, value))
      case _ => deserializationError("invalid basic type")
    }

    override def write(obj: Attribute): JsValue = {
      implicit val format = bundleValueFormat(obj.value.bundleDataType)
      JsObject(("name", JsString(obj.name)),
        ("type", obj.value.bundleDataType.toJson),
        ("value", format.write(obj.value.value)))
    }
  }

  protected implicit def bundleEmbeddedAttributeListFormat(implicit hr: HasBundleRegistry): JsonFormat[AttributeList] = new JsonFormat[AttributeList] {
    override def write(obj: AttributeList): JsValue = obj.attributes.toJson
    override def read(json: JsValue): AttributeList = AttributeList(json.convertTo[Seq[Attribute]])
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

  implicit def bundleModelFormat(implicit hr: HasBundleRegistry): RootJsonFormat[Model] = jsonFormat2(Model.apply)
  implicit val bundleNodeFormat: RootJsonFormat[Node] = jsonFormat2(Node.apply)
  implicit def bundleBundleMetaFormat(implicit hr: HasBundleRegistry): RootJsonFormat[BundleMeta] = jsonFormat5(BundleMeta)
}

/** All spray.json.RootJsonFormat formats needed for Bundle.ML JSON serialization.
  *
  * The 4 spray.json.RootJsonFormat formats provided are:
  * <ul>
  *   <li>[[JsonSupport.bundleBundleMetaFormat]]</li>
  *   <li>[[JsonSupport.bundleNodeFormat]]</li>
  *   <li>[[JsonSupport.bundleModelFormat]]</li>
  *   <li>[[JsonSupport.bundleAttributeListFormat]]</li>
  * </ul>
  *
  * These are the only 4 implicit formats needed to serialize Bundle.ML models.
  */
trait JsonSupport extends JsonSupportLowPriority {
  implicit def bundleAttributeListFormat(implicit hr: HasBundleRegistry): RootJsonFormat[AttributeList] = new RootJsonFormat[AttributeList] {
    override def read(json: JsValue): AttributeList = json match {
      case json: JsObject =>
        AttributeList(json.fields("attributes").convertTo[Seq[Attribute]])
      case _ => deserializationError("invalid attribute list")
    }

    override def write(obj: AttributeList): JsValue = JsObject("attributes" -> obj.attributes.toJson)
  }
}

object JsonSupport extends JsonSupport
