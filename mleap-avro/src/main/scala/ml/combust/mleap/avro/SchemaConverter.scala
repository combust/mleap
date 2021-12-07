package ml.combust.mleap.avro

import java.nio.charset.Charset

import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.types._
import ml.combust.mleap.tensor.{ByteString, Tensor}
import org.apache.avro.Schema

import scala.language.implicitConversions
import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}
import scala.util.Try

/**
  * Created by hollinwilkins on 10/31/16.
  */
object SchemaConverter {
  def tensorSchema[T: ClassTag] = {
    val r = Try {
      val (name, valuesSchema) = classTag[T].runtimeClass match {
        case Tensor.BooleanClass =>
          ("Boolean", Schema.createArray(Schema.create(Schema.Type.BOOLEAN)))
        case Tensor.ByteClass =>
          ("Byte", Schema.create(Schema.Type.BYTES))
        case Tensor.ShortClass =>
          ("Short", Schema.createArray(Schema.create(Schema.Type.INT)))
        case Tensor.IntClass =>
          ("Int", Schema.createArray(Schema.create(Schema.Type.INT)))
        case Tensor.LongClass =>
          ("Long", Schema.createArray(Schema.create(Schema.Type.LONG)))
        case Tensor.FloatClass =>
          ("Float", Schema.createArray(Schema.create(Schema.Type.FLOAT)))
        case Tensor.DoubleClass =>
          ("Double", Schema.createArray(Schema.create(Schema.Type.DOUBLE)))
        case Tensor.StringClass =>
          ("String", Schema.createArray(Schema.create(Schema.Type.STRING)))
        case Tensor.ByteStringClass =>
          ("ByteString", Schema.createArray(Schema.create(Schema.Type.BYTES)))
        case _ => throw new IllegalArgumentException(s"invalid base ${classTag[T].runtimeClass.getName}")
      }
      val indicesSchema = Schema.createUnion(Schema.createArray(Schema.createArray(Schema.create(Schema.Type.INT))),
        Schema.create(Schema.Type.NULL))
      Schema.createRecord(s"${name}Tensor", "", "ml.combust.mleap.avro", false,
        Seq(new Schema.Field("dimensions", Schema.createArray(Schema.create(Schema.Type.INT)), "", null: AnyRef),
          new Schema.Field("values", valuesSchema, "", null: AnyRef),
          new Schema.Field("indices", indicesSchema, "", null: AnyRef)).asJava)
    }

    r.get
  }

  private val booleanTensorSchema = tensorSchema[Boolean]
  private val byteTensorSchema = tensorSchema[Byte]
  private val shortTensorSchema = tensorSchema[Short]
  private val integerTensorSchema = tensorSchema[Int]
  private val longTensorSchema = tensorSchema[Long]
  private val floatTensorSchema = tensorSchema[Float]
  private val doubleTensorSchema = tensorSchema[Double]
  private val stringTensorSchema = tensorSchema[String]
  private val byteStringTensorSchema = tensorSchema[ByteString]

  val bytesCharset = Charset.forName("UTF-8")

  val tensorSchemaDimensionsIndex = 0
  val tensorSchemaValuesIndex = 1
  val tensorSchemaIndicesIndex = 2

  val customSchemaIndex = 0

  implicit def mleapToAvro(schema: StructType): Schema = {
    val fields = schema.fields.map(mleapToAvroField).asJava
    Schema.createRecord("LeapFrame", "", "ml.combust.mleap.avro", false, fields)
  }

  implicit def mleapToAvroField(field: StructField): Schema.Field = new Schema.Field(field.name, mleapToAvroType(field.dataType), "", null: AnyRef)

  def maybeNullableAvroType(base: Schema, isNullable: Boolean): Schema = {
    if(isNullable) {
      Schema.createUnion(base, Schema.create(Schema.Type.NULL))
    } else { base }
  }

  implicit def mleapBasicToAvroType(basicType: BasicType): Schema = basicType match {
    case BasicType.Boolean => Schema.create(Schema.Type.BOOLEAN)
    case BasicType.Byte => Schema.create(Schema.Type.INT)
    case BasicType.Short => Schema.create(Schema.Type.INT)
    case BasicType.Int => Schema.create(Schema.Type.INT)
    case BasicType.Long => Schema.create(Schema.Type.LONG)
    case BasicType.Float => Schema.create(Schema.Type.FLOAT)
    case BasicType.Double => Schema.create(Schema.Type.DOUBLE)
    case BasicType.String => Schema.create(Schema.Type.STRING)
    case BasicType.ByteString => Schema.create(Schema.Type.BYTES)
  }

  implicit def mleapToAvroType(dataType: DataType): Schema = dataType match {
    case st: ScalarType => maybeNullableAvroType(mleapBasicToAvroType(st.base), st.isNullable)
    case lt: ListType => maybeNullableAvroType(Schema.createArray(mleapBasicToAvroType(lt.base)), lt.isNullable)
    case mt: MapType =>
      // Avro Map keys are only allowed to be strings https://avro.apache.org/docs/current/spec.html#Maps
      require(mt.key == BasicType.String, s"Avro only allows String keys, found ${mt.key}")
      maybeNullableAvroType(Schema.createMap(mleapBasicToAvroType(mt.base)), mt.isNullable)
    case tt: TensorType =>
      tt.base match {
        case BasicType.Boolean => booleanTensorSchema
        case BasicType.Byte => byteTensorSchema
        case BasicType.Short => shortTensorSchema
        case BasicType.Int => integerTensorSchema
        case BasicType.Long => longTensorSchema
        case BasicType.Float => floatTensorSchema
        case BasicType.Double => doubleTensorSchema
        case BasicType.String => stringTensorSchema
        case BasicType.ByteString => byteStringTensorSchema
        case _ => throw new IllegalArgumentException(s"invalid type ${tt.base}")
      }
    case _ => throw new IllegalArgumentException(s"invalid data type: $dataType")
  }

  implicit def avroToMleap(schema: Schema)
                          (implicit context: MleapContext): StructType = schema.getType match {
    case Schema.Type.RECORD =>
      val fields = schema.getFields.asScala.map(avroToMleapField)
      StructType(fields).get
    case _ => throw new IllegalArgumentException("invalid avro record type")
  }

  implicit def avroToMleapField(field: Schema.Field)
                               (implicit context: MleapContext): StructField = StructField(field.name(), avroToMleapType(field.schema()))

  def maybeNullableMleapType(schema: Schema): DataType = {
    val types = schema.getTypes.asScala
    assert(types.size == 2, "only nullable unions supported (2 type unions)")

    types.find(_.getType == Schema.Type.NULL).flatMap {
      _ => types.find(_.getType != Schema.Type.NULL)
    }.map(avroToMleapType).getOrElse {
      throw new IllegalArgumentException(s"unsupported schema: $schema")
    }.asNullable
  }

  def avroToMleapBasicType(base: Schema.Type): BasicType = base match {
    case Schema.Type.BOOLEAN => BasicType.Boolean
    case Schema.Type.INT => BasicType.Int
    case Schema.Type.LONG => BasicType.Long
    case Schema.Type.FLOAT => BasicType.Float
    case Schema.Type.DOUBLE => BasicType.Double
    case Schema.Type.STRING => BasicType.String
    case Schema.Type.BYTES => BasicType.ByteString
    case _ => throw new IllegalArgumentException("invalid basic type")
  }

  implicit def avroToMleapType(schema: Schema)
                              (implicit context: MleapContext): DataType = schema.getType match {
    case Schema.Type.BOOLEAN => ScalarType.Boolean
    case Schema.Type.INT => ScalarType.Int
    case Schema.Type.LONG => ScalarType.Long
    case Schema.Type.FLOAT => ScalarType.Float
    case Schema.Type.DOUBLE => ScalarType.Double
    case Schema.Type.STRING => ScalarType.String
    case Schema.Type.BYTES => ScalarType.ByteString
    case Schema.Type.ARRAY => ListType(avroToMleapBasicType(schema.getElementType.getType))
    case Schema.Type.MAP =>
      // Avro Map keys are only allowed to be strings https://avro.apache.org/docs/current/spec.html#Maps
      MapType(BasicType.String, avroToMleapBasicType(schema.getValueType.getType))
    case Schema.Type.UNION => maybeNullableMleapType(schema)
    case Schema.Type.RECORD =>
      schema.getName match {
        case "BooleanTensor" => TensorType(BasicType.Boolean)
        case "ByteTensor" => TensorType(BasicType.Byte)
        case "ShortTensor" => TensorType(BasicType.Short)
        case "IntTensor" => TensorType(BasicType.Int)
        case "LongTensor" => TensorType(BasicType.Long)
        case "FloatTensor" => TensorType(BasicType.Float)
        case "DoubleTensor" => TensorType(BasicType.Double)
        case "StringTensor" => TensorType(BasicType.String)
        case "ByteStringTensor" => TensorType(BasicType.ByteString)
        case _ => throw new IllegalArgumentException("invalid avro record")
      }
    case _ => throw new IllegalArgumentException("invalid avro record")
  }
}
