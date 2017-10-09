package ml.combust.mleap.runtime.frame

import ml.combust.mleap.core.types.{ScalarType, StructType}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 10/5/17.
  */
class RowTransformerSpec extends FunSpec {
  describe("non-shuffling transforms") {
    it("correctly transforms incoming rows") {
      val udf: UserDefinedFunction = (in: Double) => in + 42.0
      (for(inputSchema <- StructType("double" -> ScalarType.Double);
           rt = RowTransformer(inputSchema);
           rt2 <- rt.withColumn("double_out", "double")(udf)) yield {
        val row = Row(32.5)
        val row2 = rt2.transform(row)

        assert(row2.getDouble(0) == 32.5)
        assert(row2.getDouble(1) == 32.5 + 42.0)
      }).get
    }
  }

  describe("shuffling transforms") {
    it("correctly unshuffles the data before returning the row") {
      val udf: UserDefinedFunction = (in: Double) => in * in
      (for(inputSchema <- StructType("double" -> ScalarType.Double);
           rt = RowTransformer(inputSchema);
           rt2 <- rt.withColumn("double_out", "double")(udf);
           rt3 <- rt2.drop("double");
           rt4 <- rt3.withColumn("double_out2", "double_out")(udf)) yield {
        val row = Row(3.0)
        val row2 = rt4.transform(row)

        assert(row2.getDouble(0) == 9.0)
        assert(row2.getDouble(1) == 81.0)
      }).get
    }

    it("correctly handle a complicated set of transformations") {
      val udf1: UserDefinedFunction = (in: Double) => in * in
      val udf2: UserDefinedFunction = (in: String) => s"HELLO WORLD!: $in"
      val udf3: UserDefinedFunction = (in1: Double, in2: Int) => (in1 + in2).toFloat
      val udf4: UserDefinedFunction = (in1: Double, in2: Int) => (in1 + in2, in1 - in2)

      val is = StructType("double" -> ScalarType.Double,
        "string" -> ScalarType.String,
        "int" -> ScalarType.Int)

      (for(inputSchema <- is;
           f = RowTransformer(inputSchema);
           f1 <- f.withColumns(Seq("out1", "out2"), "double", "int")(udf4);
           f2 <- f1.withColumn("out3", "out2", "out1")(udf3);
           f3 <- f2.drop("out2", "out1");
           f4 <- f3.withColumn("out4", "string")(udf2);
           f5 <- f4.select("out3", "out4", "string");
           f6 <- f5.withColumn("out5", "out3")(udf1)) yield {
        val row = Row(42.0, "NO", 23)
        val row2 = f6.transform(row)

        assert(row2.getFloat(0) == 84.0)
        assert(row2.getString(1) == "HELLO WORLD!: NO")
        assert(row2.getString(2) == "NO")
        assert(row2.getDouble(3) == 84.0 * 84.0)
      }).get
    }
  }
}
