package ml.combust.mleap.json

import ml.combust.mleap.core.{DenseTensor, SparseTensor, Tensor}
import ml.combust.mleap.runtime.{DefaultLeapFrame, LeapFrame, MleapContext}
import ml.combust.mleap.runtime.types._
import spray.json.DefaultJsonProtocol._
import spray.json._
import spray.json.{JsValue, JsonFormat}

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Created by hollinwilkins on 8/23/16.
  */
trait JsonSupport {
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
      case "float" => FloatType(isNullable)
      case "double" => DoubleType(isNullable)
      case "string" => StringType(isNullable)
      case "long" => LongType(isNullable)
      case "boolean" => BooleanType(isNullable)
      case "integer" => IntegerType(isNullable)
    }

    override def write(obj: BasicType): JsValue = obj match {
      case FloatType(isNullable) => writeMaybeNullable(JsString("float"), isNullable)
      case DoubleType(isNullable) => writeMaybeNullable(JsString("double"), isNullable)
      case StringType(isNullable) => writeMaybeNullable(JsString("string"), isNullable)
      case LongType(isNullable) => writeMaybeNullable(JsString("long"), isNullable)
      case BooleanType(isNullable) => writeMaybeNullable(JsString("boolean"), isNullable)
      case IntegerType(isNullable) => writeMaybeNullable(JsString("integer"), isNullable)
    }

    override def read(json: JsValue): BasicType = json match {
      case JsString(name) => forName(name)
      case json: JsObject => readNullable(json)
      case _ => throw new Error("Invalid basic type") // TODO: better error
    }
  })

  implicit val mleapCustomTypeWriterFormat: JsonWriter[CustomType] = new JsonWriter[CustomType] {
    override def write(obj: CustomType): JsValue = JsObject("type" -> JsString("custom"),
      "custom" -> JsString(obj.name))
  }

  implicit def mleapCustomTypeReaderFormat(implicit context: MleapContext): JsonReader[CustomType] = new JsonReader[CustomType] {
    override def read(json: JsValue): CustomType = {
      val obj = json.asJsObject("invalid custom type")

      obj.fields("custom") match {
        case JsString(custom) => context.customTypeForAlias(custom)
        case _ => deserializationError("invalid custom type")
      }
    }
  }

  implicit val mleapDataTypeWriterFormat: JsonWriter[DataType] = new JsonWriter[DataType] {
    override def write(obj: DataType): JsValue = obj match {
      case bt: BasicType => mleapBasicTypeFormat.write(bt)
      case lt: ListType => mleapListTypeWriterFormat.write(lt)
      case tt: TensorType => mleapTensorTypeFormat.write(tt)
      case ct: CustomType => mleapCustomTypeWriterFormat.write(ct)
      case AnyType(_) => serializationError("AnyType not supported for JSON serialization")
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
          case Some(JsString("custom")) => mleapCustomTypeReaderFormat.read(obj)
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

  implicit def mleapArrayFormat[T: JsonFormat: ClassTag]: RootJsonFormat[Array[T]] = new RootJsonFormat[Array[T]] {
    val base = implicitly[JsonFormat[T]]

    override def write(obj: Array[T]): JsValue = {
      JsArray(obj.map(base.write): _*)
    }

    override def read(json: JsValue): Array[T] = json match {
      case json: JsArray =>
        val elements = json.elements
        val size = elements.size
        val values = new Array[T](size)
        (0 until size).foreach(i => values(i) = base.read(elements(i)))
        values
      case _ => deserializationError("invalid array")
    }
  }

  implicit def mleapDenseTensorFormat[T: JsonFormat: ClassTag]: RootJsonFormat[DenseTensor[T]] = jsonFormat2(DenseTensor[T])
  implicit def mleapSparseTensorFormat[T: JsonFormat: ClassTag]: RootJsonFormat[SparseTensor[T]] = jsonFormat3(SparseTensor[T])
  implicit def mleapTensorFormat[T: JsonFormat: ClassTag]: RootJsonFormat[Tensor[T]] = new RootJsonFormat[Tensor[T]] {
    override def write(obj: Tensor[T]): JsValue = obj match {
      case obj: DenseTensor[_] => obj.asInstanceOf[DenseTensor[T]].toJson
      case obj: SparseTensor[_] => obj.asInstanceOf[SparseTensor[T]].toJson
    }

    override def read(json: JsValue): Tensor[T] = json match {
      case json: JsObject =>
        if(json.fields.contains("indices")) {
          mleapSparseTensorFormat[T].read(json)
        } else {
          mleapDenseTensorFormat[T].read(json)
        }
      case _ => deserializationError("invalid tensor")
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
