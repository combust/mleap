package ml.combust.mleap.json

import ml.combust.mleap.core.types._
import JsonSupport._
import spray.json._
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/25/17.
  */
class SchemaSpec extends FunSpec {
  private val BASIC_TYPES = Seq(
    BasicType.Boolean,
    BasicType.Byte,
    BasicType.Short,
    BasicType.Int,
    BasicType.Long,
    BasicType.Float,
    BasicType.Double,
    BasicType.String,
    BasicType.ByteString
  )

  private val DATA_TYPES: Seq[DataType] = BASIC_TYPES.flatMap {
    bt =>
      Seq[DataType](ScalarType(bt).asNullable,
        ScalarType(bt).nonNullable,
        ListType(bt).asNullable,
        ListType(bt).nonNullable,
        TensorType(bt, Seq(23, 44)).asNullable,
        TensorType(bt, Seq(23, 44)).nonNullable)
  }

  describe("DataType -> JSON -> DataType") {
    for(dt <- DATA_TYPES) {
      it(s"$dt retains all information") {
        assert(dt.toJson.convertTo[DataType] == dt)
      }
    }
  }
}
