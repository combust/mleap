package ml.combust.mleap.core.types

import ml.combust.mleap.tensor.{ByteString, Tensor}
import org.scalatest.FunSpec
import scala.util.Success

/**
  * Created by hollinwilkins on 9/18/17.
  */
class CastingSpec extends FunSpec {
  def createValue(base: BasicType, value: Double): Any = base match {
    case BasicType.Boolean => if (value == 0) false else true
    case BasicType.Byte => value.toByte
    case BasicType.Short => value.toShort
    case BasicType.Int => value.toInt
    case BasicType.Long => value.toLong
    case BasicType.Float => value.toFloat
    case BasicType.Double => value
    case BasicType.String => value.toString
    case BasicType.ByteString => ByteString(value.toString.getBytes)
  }

  val castTests = Seq(
    (BasicType.Boolean, BasicType.Boolean, true, true),
    (BasicType.Boolean, BasicType.Byte, true, 1.toByte),
    (BasicType.Boolean, BasicType.Short, true, 1.toShort),
    (BasicType.Boolean, BasicType.Int, true, 1),
    (BasicType.Boolean, BasicType.Long, true, 1.toLong),
    (BasicType.Boolean, BasicType.Float, true, 1.0.toFloat),
    (BasicType.Boolean, BasicType.Double, true, 1.0),
    (BasicType.Boolean, BasicType.String, true, "1"),

    (BasicType.Byte, BasicType.Byte, 7.toByte, 7.toByte),
    (BasicType.Byte, BasicType.Boolean, 7.toByte, true),
    (BasicType.Byte, BasicType.Boolean, 0.toByte, false),
    (BasicType.Byte, BasicType.Short, 13.toByte, 13.toShort),
    (BasicType.Byte, BasicType.Int, 13.toByte, 13),
    (BasicType.Byte, BasicType.Long, 13.toByte, 13.toLong),
    (BasicType.Byte, BasicType.Float, 13.toByte, 13.toFloat),
    (BasicType.Byte, BasicType.Double, 13.toByte, 13.toDouble),
    (BasicType.Byte, BasicType.String, 13.toByte, 13.toString),

    (BasicType.Short, BasicType.Short, 7.toShort, 7.toShort),
    (BasicType.Short, BasicType.Boolean, 7.toShort, true),
    (BasicType.Short, BasicType.Boolean, 0.toShort, false),
    (BasicType.Short, BasicType.Byte, 13.toShort, 13.toByte),
    (BasicType.Short, BasicType.Int, 13.toShort, 13),
    (BasicType.Short, BasicType.Long, 13.toShort, 13.toLong),
    (BasicType.Short, BasicType.Float, 13.toShort, 13.toFloat),
    (BasicType.Short, BasicType.Double, 13.toShort, 13.toDouble),
    (BasicType.Short, BasicType.String, 13.toShort, 13.toString),

    (BasicType.Int, BasicType.Int, 7, 7),
    (BasicType.Int, BasicType.Boolean, 7, true),
    (BasicType.Int, BasicType.Boolean, 0, false),
    (BasicType.Int, BasicType.Byte, 13, 13.toByte),
    (BasicType.Int, BasicType.Short, 13, 13.toShort),
    (BasicType.Int, BasicType.Long, 13, 13.toLong),
    (BasicType.Int, BasicType.Float, 13, 13.toFloat),
    (BasicType.Int, BasicType.Double, 13, 13.toDouble),
    (BasicType.Int, BasicType.String, 13, 13.toString),

    (BasicType.Long, BasicType.Long, 7.toLong, 7.toLong),
    (BasicType.Long, BasicType.Boolean, 7.toLong, true),
    (BasicType.Long, BasicType.Boolean, 0.toLong, false),
    (BasicType.Long, BasicType.Byte, 13.toLong, 13.toByte),
    (BasicType.Long, BasicType.Short, 13.toLong, 13.toShort),
    (BasicType.Long, BasicType.Int, 13.toLong, 13),
    (BasicType.Long, BasicType.Float, 13.toLong, 13.toFloat),
    (BasicType.Long, BasicType.Double, 13.toLong, 13.toDouble),
    (BasicType.Long, BasicType.String, 13.toLong, 13.toString),

    (BasicType.Float, BasicType.Float, 7.0.toFloat, 7.0.toFloat),
    (BasicType.Float, BasicType.Boolean, 7.0.toFloat, true),
    (BasicType.Float, BasicType.Boolean, 0.0.toFloat, false),
    (BasicType.Float, BasicType.Byte, 13.0.toFloat, 13.0.toFloat.toByte),
    (BasicType.Float, BasicType.Short, 13.0.toFloat, 13.0.toFloat.toShort),
    (BasicType.Float, BasicType.Int, 13.0.toFloat, 13.0.toFloat.toInt),
    (BasicType.Float, BasicType.Long, 13.0.toFloat, 13.0.toLong),
    (BasicType.Float, BasicType.Double, 13.0.toFloat, 13.0),
    (BasicType.Float, BasicType.String, 13.toFloat, 13.0.toFloat.toString),

    (BasicType.Double, BasicType.Double, 7.0, 7.0),
    (BasicType.Double, BasicType.Boolean, 7.0, true),
    (BasicType.Double, BasicType.Boolean, 0.0, false),
    (BasicType.Double, BasicType.Byte, 13.0, 13.0.toByte),
    (BasicType.Double, BasicType.Short, 13.0, 13.0.toShort),
    (BasicType.Double, BasicType.Int, 13.0, 13.0.toInt),
    (BasicType.Double, BasicType.Long, 13.0, 13.0.toLong),
    (BasicType.Double, BasicType.Float, 13.0, 13.0.toFloat),
    (BasicType.Double, BasicType.String, 13.0, 13.0.toString),

    (BasicType.String, BasicType.String, "hello", "hello"),
    (BasicType.String, BasicType.Boolean, "true", true),
    (BasicType.String, BasicType.Boolean, "false", false),
    (BasicType.String, BasicType.Boolean, "", false),
    (BasicType.String, BasicType.Byte, "12", 12.toByte),
    (BasicType.String, BasicType.Short, "77", 77.toShort),
    (BasicType.String, BasicType.Int, "789", 789),
    (BasicType.String, BasicType.Long, "789", 789.toLong),
    (BasicType.String, BasicType.Float, "14.5", 14.5.toFloat),
    (BasicType.String, BasicType.Double, "16.7", 16.7),
    (BasicType.String, BasicType.Byte, "null", null),
    (BasicType.String, BasicType.Short, "null", null),
    (BasicType.String, BasicType.Int, "null", null),
    (BasicType.String, BasicType.Long, "null", null),
    (BasicType.String, BasicType.Float, "null", null),
    (BasicType.String, BasicType.Double, "null", null),
    (BasicType.String, BasicType.Byte, "", null),
    (BasicType.String, BasicType.Short, "", null),
    (BasicType.String, BasicType.Int, "", null),
    (BasicType.String, BasicType.Long, "", null),
    (BasicType.String, BasicType.Float, "", null),
    (BasicType.String, BasicType.Double, "", null)
  )

  def createTensor(base: BasicType, values: Seq[_]): Tensor[_] = base match {
    case BasicType.Boolean => Tensor.denseVector(values.map(_.asInstanceOf[Boolean]).toArray)
    case BasicType.Byte => Tensor.denseVector(values.map(_.asInstanceOf[Byte]).toArray)
    case BasicType.Short => Tensor.denseVector(values.map(_.asInstanceOf[Short]).toArray)
    case BasicType.Int => Tensor.denseVector(values.map(_.asInstanceOf[Int]).toArray)
    case BasicType.Long => Tensor.denseVector(values.map(_.asInstanceOf[Long]).toArray)
    case BasicType.Float => Tensor.denseVector(values.map(_.asInstanceOf[Float]).toArray)
    case BasicType.Double => Tensor.denseVector(values.map(_.asInstanceOf[Double]).toArray)
    case BasicType.String => Tensor.denseVector(values.map(_.asInstanceOf[String]).toArray)
    case BasicType.ByteString => Tensor.denseVector(values.map(_.asInstanceOf[ByteString]).toArray)
  }

  def createTensorScalar(base: BasicType, value: Any): Tensor[_] = base match {
    case BasicType.Boolean => Tensor.scalar(value.asInstanceOf[Boolean])
    case BasicType.Byte => Tensor.scalar(value.asInstanceOf[Byte])
    case BasicType.Short => Tensor.scalar(value.asInstanceOf[Short])
    case BasicType.Int => Tensor.scalar(value.asInstanceOf[Int])
    case BasicType.Long => Tensor.scalar(value.asInstanceOf[Long])
    case BasicType.Float => Tensor.scalar(value.asInstanceOf[Float])
    case BasicType.Double => Tensor.scalar(value.asInstanceOf[Double])
    case BasicType.String => Tensor.scalar(value.asInstanceOf[String])
    case BasicType.ByteString => Tensor.scalar(value.asInstanceOf[ByteString])
  }

  for(((from, to, fromValue, expectedValue), i) <- castTests.zipWithIndex) {
    describe(s"cast from $from to $to - $i") {
      it("casts the scalar") {
        val c = Casting.cast(ScalarType(from), ScalarType(to)).getOrElse(Success((v: Any) => v)).get
        val oc = Casting.cast(ScalarType(from), ScalarType(to).nonNullable).getOrElse(Success((v: Any) => v)).get
        val co = Casting.cast(ScalarType(from).nonNullable, ScalarType(to)).getOrElse(Success((v: Any) => v)).get
        val oco = Casting.cast(ScalarType(from), ScalarType(to)).getOrElse(Success((v: Any) => v)).get

        assert(c(fromValue) == expectedValue)
        assertThrows[NullPointerException](oc(null))
        assert(co(fromValue) == expectedValue)
        assert(oco(null) == null)
      }

      it("casts the list") {
        val fromList = Seq(fromValue, fromValue, fromValue)
        val expectedList = Seq(expectedValue, expectedValue, expectedValue)
        val expectedTensor = createTensor(to, expectedList)

        val c = Casting.cast(ListType(from), ListType(to)).getOrElse(Success((v: Any) => v)).get
        val oc = Casting.cast(ListType(from), ListType(to).nonNullable).getOrElse(Success((v: Any) => v)).get
        val co = Casting.cast(ListType(from).nonNullable, ListType(to)).getOrElse(Success((v: Any) => v)).get
        val oco = Casting.cast(ListType(from), ListType(to)).getOrElse(Success((v: Any) => v)).get
        val tcl = Casting.cast(ListType(from), TensorType(to, Some(Seq(expectedList.length)))).getOrElse(Success((v: Any) => v)).get

        assert(c(fromList) == expectedList)
        assertThrows[NullPointerException](oc(null))
        assert(co(fromList) == expectedList)
        assert(oco(null) == null)
        assert(tcl(fromList) == expectedTensor)
      }

      it("casts the tensor") {
        val fromTensor = createTensor(from, Seq(fromValue))
        val expectedTensor = createTensor(to, Seq(expectedValue))

        val fromScalarTensor = createTensorScalar(from, fromValue)
        val expectedScalarTensor = createTensorScalar(to, expectedValue)

        val fromListTensor = createTensor(from, Seq(fromValue, fromValue, fromValue))
        val expectedList = Seq(expectedValue, expectedValue, expectedValue)

        val c = Casting.cast(TensorType(from), TensorType(to)).getOrElse(Success((v: Any) => v)).get
        val oc = Casting.cast(TensorType(from), TensorType(to).nonNullable).getOrElse(Success((v: Any) => v)).get
        val co = Casting.cast(TensorType(from).nonNullable, TensorType(to)).getOrElse(Success((v: Any) => v)).get
        val oco = Casting.cast(TensorType(from), TensorType(to)).getOrElse(Success((v: Any) => v)).get
        val tc = Casting.cast(ScalarType(from), TensorType(to, Some(Seq()))).getOrElse(Success((v: Any) => v)).get
        val ct = Casting.cast(TensorType(from, Some(Seq())), ScalarType(to)).getOrElse(Success((v: Any) => v)).get
        val lct = Casting.cast(TensorType(from, Some(Seq(expectedList.length))), ListType(to)).getOrElse(Success((v: Any) => v)).get

        assert(c(fromTensor) == expectedTensor)
        assertThrows[NullPointerException](oc(null))
        assert(co(fromTensor) == expectedTensor)
        assert(oco(null) == null)
        assert(tc(fromValue) == expectedScalarTensor)
        assert(ct(fromScalarTensor) == expectedValue)
        assert(lct(fromListTensor) == expectedList)
      }

    }
  }
}
