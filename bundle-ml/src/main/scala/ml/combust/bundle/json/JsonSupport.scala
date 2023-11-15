package ml.combust.bundle.json

import java.util.{Base64, UUID}

import com.google.protobuf.ByteString
import ml.bundle._
import ml.combust.bundle.tensor._
import ml.combust.mleap.tensor
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.mutable

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
      case JsString("unknown") => BasicType.UNDEFINED
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
    override def write(obj: ByteString): JsValue = {
      JsString(Base64.getEncoder.encodeToString(obj.toByteArray))
    }

    override def read(json: JsValue): ByteString = json match {
      case JsString(b64) => ByteString.copyFrom(Base64.getDecoder.decode(b64))
      case _ => deserializationError("invalid byte string")
    }
  }

  implicit val TensorByteStringFormat: JsonFormat[tensor.ByteString] = new JsonFormat[tensor.ByteString] {
    override def write(obj: tensor.ByteString): JsValue = {
      JsString(Base64.getEncoder.encodeToString(obj.bytes))
    }

    override def read(json: JsValue): tensor.ByteString = json match {
      case JsString(b64) => tensor.ByteString(Base64.getDecoder.decode(b64))
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

  implicit val bundleDataTypeFormat: JsonFormat[DataType] = jsonFormat2(DataType.apply)
  implicit val bundleFieldFormat: JsonFormat[Field] = jsonFormat2(Field.apply)
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

  implicit val bundleScalarFormat: JsonFormat[Scalar] = new JsonFormat[Scalar] {
    override def write(obj: Scalar): JsValue = {
      val fb = mutable.Seq.newBuilder[(String, JsValue)]

      if(obj.b) { fb += ("boolean" -> obj.b.toJson) }
      if(obj.i != 0) { fb += ("int" -> obj.i.toJson) }
      if(obj.l != 0) { fb += ("long" -> obj.l.toJson) }
      if(obj.f != 0) { fb += ("float" -> obj.f.toJson) }
      if(obj.d != 0) { fb += ("double" -> obj.d.toJson) }
      if(obj.s != "") { fb += ("string" -> obj.s.toJson) }
      if(obj.bs != ByteString.EMPTY) { fb += ("byte_string" -> obj.bs.toJson) }
      obj.t.foreach(t => fb += ("tensor" -> t.toJson))
      if(obj.bt != BasicType.UNDEFINED) { fb += ("basic_type" -> obj.bt.toJson) }
      obj.ds.foreach(ds => fb += ("data_shape" -> ds.toJson))
      obj.m.foreach(m => fb += ("model" -> m.toJson))

      JsObject(fb.result().toSeq: _*)
    }

    override def read(json: JsValue): Scalar = json match {
      case JsObject(fields) => Scalar(
        b = fields.getOrElse("boolean", JsBoolean(false)).convertTo[Boolean],
        i = fields.getOrElse("int", JsNumber(0)).convertTo[Int],
        l = fields.getOrElse("long", JsNumber(0)).convertTo[Long],
        f = fields.getOrElse("float", JsNumber(0)).convertTo[Float],
        d = fields.getOrElse("double", JsNumber(0)).convertTo[Double],
        s = fields.getOrElse("string", JsString("")).convertTo[String],
        bs = fields.getOrElse("byte_string", JsString("")).convertTo[ByteString],
        t = fields.getOrElse("tensor", JsNull).convertTo[Option[Tensor]],
        bt = fields.getOrElse("basic_type", JsString("unknown")).convertTo[BasicType],
        ds = fields.getOrElse("data_shape", JsNull).convertTo[Option[DataShape]],
        m = fields.getOrElse("model", JsNull).convertTo[Option[Model]]
      )
      case _ => deserializationError(s"invalid scalar value $json")
    }
  }

  implicit val bundleTensorFormat: JsonFormat[Tensor] = new JsonFormat[Tensor] {
    override def write(obj: Tensor): JsValue = {
      val value = obj.base match {
        case BasicType.BOOLEAN => "boolean" -> BooleanArraySerializer.read(obj.value.toByteArray).toJson
        case BasicType.BYTE => "byte" -> obj.value.toByteArray.toJson
        case BasicType.SHORT => "short" -> ShortArraySerializer.read(obj.value.toByteArray).toJson
        case BasicType.INT => "int" -> IntArraySerializer.read(obj.value.toByteArray).toJson
        case BasicType.LONG => "long" -> LongArraySerializer.read(obj.value.toByteArray).toJson
        case BasicType.FLOAT => "float" -> FloatArraySerializer.read(obj.value.toByteArray).toJson
        case BasicType.DOUBLE => "double" -> DoubleArraySerializer.read(obj.value.toByteArray).toJson
        case BasicType.BYTE_STRING => "byte_string" -> ByteStringArraySerializer.read(obj.value.toByteArray).toJson
        case BasicType.STRING => "string" -> StringArraySerializer.read(obj.value.toByteArray).toJson
        case _ => serializationError(s"Invalid tensor type: ${obj.base}")
      }

      JsObject(value, "shape" -> obj.shape.get.toJson)
    }

    override def read(json: JsValue): Tensor = json match {
      case JsObject(fields) =>
        val (base, value) = if(fields.contains("boolean")) {
          (BasicType.BOOLEAN, BooleanArraySerializer.write(fields("boolean").convertTo[Array[Boolean]]))
        } else if(fields.contains("byte")) {
          (BasicType.BYTE, ByteArraySerializer.write(fields("byte").convertTo[Array[Byte]]))
        } else if(fields.contains("short")) {
          (BasicType.SHORT, ShortArraySerializer.write(fields("short").convertTo[Array[Short]]))
        } else if(fields.contains("int")) {
          (BasicType.INT, IntArraySerializer.write(fields("int").convertTo[Array[Int]]))
        } else if(fields.contains("long")) {
          (BasicType.LONG, LongArraySerializer.write(fields("long").convertTo[Array[Long]]))
        } else if(fields.contains("float")) {
          (BasicType.FLOAT, FloatArraySerializer.write(fields("float").convertTo[Array[Float]]))
        } else if(fields.contains("double")) {
          (BasicType.DOUBLE, DoubleArraySerializer.write(fields("double").convertTo[Array[Double]]))
        } else if(fields.contains("byte_string")) {
          (BasicType.BYTE_STRING, ByteStringArraySerializer.write(fields("byte_string").convertTo[Array[tensor.ByteString]]))
        } else if(fields.contains("string")) {
          (BasicType.STRING, StringArraySerializer.write(fields("string").convertTo[Array[String]]))
        } else {
          deserializationError("Invalid tensor")
        }
        val shape = fields("shape").convertTo[TensorShape]

        Tensor(base = base,
          value = ByteString.copyFrom(value),
          shape = Some(shape))
      case _ => deserializationError("Invalid tensor")
    }
  }

  implicit val bundleListFormat: JsonFormat[List] = new JsonFormat[List] {
    override def write(obj: List): JsValue = {
      val fb = mutable.Seq.newBuilder[(String, JsValue)]

      if(obj.b.nonEmpty) { fb += ("boolean" -> obj.b.toJson) }
      if(obj.i.nonEmpty) { fb += ("int" -> obj.i.toJson) }
      if(obj.l.nonEmpty) { fb += ("long" -> obj.l.toJson) }
      if(obj.f.nonEmpty) { fb += ("float" -> obj.f.toJson) }
      if(obj.d.nonEmpty) { fb += ("double" -> obj.d.toJson) }
      if(obj.s.nonEmpty) { fb += ("string" -> obj.s.toJson) }
      if(obj.bs.nonEmpty) { fb += ("byte_string" -> obj.bs.toJson) }
      if(obj.t.nonEmpty) { fb += ("tensor" -> obj.t.toJson) }
      if(obj.bt.nonEmpty) { fb += ("basic_type" -> obj.bt.toJson) }
      if(obj.ds.nonEmpty) { fb += ("data_shape" -> obj.ds.toJson) }
      if(obj.m.nonEmpty) { fb += ("model" -> obj.m.toJson) }

      JsObject(fb.result().toSeq: _*)
    }

    override def read(json: JsValue): List = json match {
      case JsObject(fields) => List(
        b = fields.getOrElse("boolean", JsArray()).convertTo[Seq[Boolean]],
        i = fields.getOrElse("int", JsArray()).convertTo[Seq[Int]],
        l = fields.getOrElse("long", JsArray()).convertTo[Seq[Long]],
        f = fields.getOrElse("float", JsArray()).convertTo[Seq[Float]],
        d = fields.getOrElse("double", JsArray()).convertTo[Seq[Double]],
        s = fields.getOrElse("string", JsArray()).convertTo[Seq[String]],
        bs = fields.getOrElse("byte_string", JsArray()).convertTo[Seq[ByteString]],
        t = fields.getOrElse("tensor", JsArray()).convertTo[Seq[Tensor]],
        bt = fields.getOrElse("basic_type", JsArray()).convertTo[Seq[BasicType]],
        ds = fields.getOrElse("data_shape", JsArray()).convertTo[Seq[DataShape]],
        m = fields.getOrElse("model", JsArray()).convertTo[Seq[Model]]
      )
      case _ => deserializationError(s"invalid scalar value $json")
    }
  }

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
  implicit val bundleBundleInfoFormat: RootJsonFormat[Bundle] = jsonFormat6(Bundle.apply)
}
object JsonSupport extends JsonSupport