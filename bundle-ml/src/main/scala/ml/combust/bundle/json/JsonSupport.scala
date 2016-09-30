package ml.combust.bundle.json

import com.google.protobuf.ByteString
import ml.bundle.Attribute.Attribute
import ml.bundle.AttributeList.AttributeList
import ml.bundle.BasicType.BasicType
import ml.bundle.BundleDef.BundleDef
import ml.bundle.BundleDef.BundleDef.Format
import ml.bundle.DataType.DataType
import ml.bundle.DataType.DataType.ListType
import ml.bundle.ModelDef.ModelDef
import ml.bundle.NodeDef.NodeDef
import ml.bundle.Shape.Shape
import ml.bundle.Socket.Socket
import ml.bundle.Tensor.Tensor
import ml.bundle.TensorType.TensorType
import ml.bundle.Value.Value.ListValue
import ml.bundle.Value.Value
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, _}

/** Low priority implicit spray.json.JsonFormat formats for protobuf objects in Bundle.ML.
  *
  * Includes many spray.json.JsonFormat format implicits as well as several
  * spray.json.RootJsonFormat format implicits for top-level JSON objects
  * such as [[ml.bundle.ModelDef.ModelDef]] and [[ml.bundle.BundleDef.BundleDef]].
  *
  * There are no members that need to be overriden if using this trait as a mixin.
 */
trait JsonLowPrioritySupport {
  protected implicit val bundleBasicTypeFormat: JsonFormat[BasicType] = new JsonFormat[BasicType] {
    override def read(json: JsValue): BasicType = json match {
      case JsString("double") => BasicType.DOUBLE
      case JsString("string") => BasicType.STRING
      case JsString("boolean") => BasicType.BOOLEAN
      case JsString("long") => BasicType.LONG
      case _ => throw new Error("invalid basic type") // TODO: better error
    }

    override def write(obj: BasicType): JsValue = obj match {
      case BasicType.DOUBLE => JsString("double")
      case BasicType.STRING => JsString("string")
      case BasicType.BOOLEAN => JsString("boolean")
      case BasicType.LONG => JsString("long")
      case _ => throw new Error("invalid basic type") // TODO: better error
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
          case _ => throw new Error("invalid data type")
        }
      case _ => throw new Error("invalid data type") // TODO: better error
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
        throw new Error("invalid data type")
      }
    }
  }

  protected implicit val bundleSocketFormat: JsonFormat[Socket] = jsonFormat2(Socket.apply)
  protected implicit val bundleShapeFormat: JsonFormat[Shape] = jsonFormat2(Shape.apply)

  protected def bundleTensorFormat(tt: TensorType): JsonFormat[Tensor] = new JsonFormat[Tensor] {
    override def read(json: JsValue): Tensor = {
      tt.base match {
        case BasicType.DOUBLE => Tensor(doubleVal = json.convertTo[Seq[Double]])
        case BasicType.STRING => Tensor(stringVal = json.convertTo[Seq[String]])
        case _ => throw new Error("unsupported tensor")
      }
    }

    override def write(obj: Tensor): JsValue = {
      tt.base match {
        case BasicType.DOUBLE => obj.doubleVal.toJson
        case BasicType.STRING => obj.stringVal.toJson
        case _ => throw new Error("unsupported tensor")
      }
    }
  }

  protected def bundleListValueFormat(lt: ListType): JsonFormat[ListValue] = new JsonFormat[ListValue] {
    val base = lt.base.get
    override def write(obj: ListValue): JsValue = {
      if(base.underlying.isCustom) {
        JsArray(obj.custom.map(b => new String(b.toByteArray).parseJson): _*)
      } else if(base.underlying.isTensor) {
        val format = bundleTensorFormat(base.getTensor)
        JsArray(obj.tensor.map(format.write): _*)
      } else if(base.underlying.isBasic) {
        base.getBasic match {
          case BasicType.DOUBLE => obj.f.toJson
          case BasicType.STRING => obj.s.toJson
          case BasicType.BOOLEAN => obj.b.toJson
          case BasicType.LONG => obj.i.toJson
          case _ => throw new Error("invalid basic type") // TODO: better error
        }
      } else if(base.underlying.isList) {
        val format = bundleListValueFormat(base.getList)
        JsArray(obj.list.map(format.write): _*)
      } else { throw new Error("invalid list") } // TODO: better error
    }

    override def read(json: JsValue): ListValue = {
      if(base.underlying.isCustom) {
        ListValue(custom = json.convertTo[Seq[JsValue]].map(_.compactPrint.getBytes).map(ByteString.copyFrom))
      } else if(base.underlying.isTensor) {
        implicit val format = bundleTensorFormat(base.getTensor)
        ListValue(tensor = json.convertTo[Seq[Tensor]])
      } else if(base.underlying.isBasic) {
        base.getBasic match {
          case BasicType.DOUBLE => ListValue(f = json.convertTo[Seq[Double]])
          case BasicType.STRING => ListValue(s = json.convertTo[Seq[String]])
          case BasicType.BOOLEAN => ListValue(b = json.convertTo[Seq[Boolean]])
          case BasicType.LONG => ListValue(i = json.convertTo[Seq[Long]])
          case _ => throw new Error("invalid basic type") // TODO: better error
        }
      } else if(base.underlying.isList) {
        implicit val format = bundleListValueFormat(base.getList)
        ListValue(list = json.convertTo[Seq[ListValue]])
      } else { throw new Error("invalid list") } // TODO: better error
    }
  }

  protected def bundleValueFormat(dt: DataType): JsonFormat[Value] = new JsonFormat[Value] {
    override def write(obj: Value): JsValue = {
      if(dt.underlying.isCustom) {
        new String(obj.getCustom.toByteArray).parseJson
      } else if(dt.underlying.isList) {
        bundleListValueFormat(dt.getList).write(obj.getList)
      } else if(dt.underlying.isTensor) {
        bundleTensorFormat(dt.getTensor).write(obj.getTensor)
      } else if(dt.underlying.isBasic) {
        dt.getBasic match {
          case BasicType.DOUBLE => DoubleJsonFormat.write(obj.getF)
          case BasicType.STRING => StringJsonFormat.write(obj.getS)
          case BasicType.BOOLEAN => BooleanJsonFormat.write(obj.getB)
          case BasicType.LONG => LongJsonFormat.write(obj.getI)
          case _ => throw new Error("invalid basic type") // TODO: better error
        }
      } else { throw new Error("unsupported data type") }
    }

    override def read(json: JsValue): Value = {
      val v = if(dt.underlying.isCustom) {
        Value.V.Custom(ByteString.copyFrom(json.compactPrint.getBytes))
      } else if(dt.underlying.isList) {
        Value.V.List(bundleListValueFormat(dt.getList).read(json))
      } else if(dt.underlying.isTensor) {
        Value.V.Tensor(bundleTensorFormat(dt.getTensor).read(json))
      } else if(dt.underlying.isBasic) {
        dt.getBasic match {
          case BasicType.DOUBLE => Value.V.F(DoubleJsonFormat.read(json))
          case BasicType.STRING => Value.V.S(StringJsonFormat.read(json))
          case BasicType.BOOLEAN => Value.V.B(BooleanJsonFormat.read(json))
          case BasicType.LONG => Value.V.I(LongJsonFormat.read(json))
          case _ => throw new Error("invalid basic type") // TODO: better error
        }
      } else { throw new Error("unsupported data type") } // TODO: better error

      Value(v)
    }
  }

  protected implicit val bundleAttributeFormat: JsonFormat[Attribute] = new JsonFormat[Attribute] {
    override def read(json: JsValue): Attribute = {
      val obj = json.asJsObject("invalid attribute") // TODO: better error
      val name = StringJsonFormat.read(obj.fields("name"))
      val dt = bundleDataTypeFormat.read(obj.fields("type"))
      val value = bundleValueFormat(dt).read(obj.fields("value"))

      Attribute(name = name, `type` = Some(dt), value = Some(value))
    }

    override def write(obj: Attribute): JsValue = {
      implicit val format = bundleValueFormat(obj.`type`.get)
      JsObject(("name", JsString(obj.name)),
        ("type", bundleDataTypeFormat.write(obj.`type`.get)),
        ("value", format.write(obj.value.get)))
    }
  }

  implicit val bundleFormatFormat: JsonFormat[Format] = new JsonFormat[Format] {
    override def write(obj: Format): JsValue = obj match {
      case Format.MIXED => JsString("mixed")
      case Format.PROTOBUF => JsString("protobuf")
      case Format.JSON => JsString("json")
      case _ => throw new Error("invalid format") // TODO: better error
    }

    override def read(json: JsValue): Format = json match {
      case JsString("mixed") => Format.MIXED
      case JsString("json") => Format.JSON
      case JsString("protobuf") => Format.PROTOBUF
      case _ => throw new Error("invalid format") // TODO: better error
    }
  }

  protected implicit val bundleEmbeddedAttributeListFormat: JsonFormat[AttributeList] = new JsonFormat[AttributeList] {
    override def write(obj: AttributeList): JsValue = obj.attributes.toJson
    override def read(json: JsValue): AttributeList = AttributeList(json.convertTo[Seq[Attribute]])
  }

  implicit val bundleModelDefFormat: RootJsonFormat[ModelDef] = jsonFormat2(ModelDef.apply)
  implicit val bundleNodeDefFormat: RootJsonFormat[NodeDef] = jsonFormat2(NodeDef.apply)
  implicit val bundleBundleDefFormat: RootJsonFormat[BundleDef] = jsonFormat5(BundleDef.apply)
}

/** All spray.json.RootJsonFormat formats needed for Bundle.ML JSON serialization.
  *
  * The 4 spray.json.RootJsonFormat formats provided are:
  * <ul>
  *   <li>[[JsonSupport.bundleBundleDefFormat]]</li>
  *   <li>[[JsonSupport.bundleNodeDefFormat]]</li>
  *   <li>[[JsonSupport.bundleModelDefFormat]]</li>
  *   <li>[[JsonSupport.bundleAttributeListFormat]]</li>
  * </ul>
  *
  * These are the only 4 implicit formats needed to serialize Bundle.ML models.
  */
trait JsonSupport extends JsonLowPrioritySupport {
  implicit val bundleAttributeListFormat: RootJsonFormat[AttributeList] = jsonFormat1(AttributeList.apply)
}
object JsonSupport extends JsonSupport
