package ml.combust.mleap.runtime.frame

import java.lang.Iterable

import ml.combust.mleap.core.types.{BasicType, StructField, StructType}
import ml.combust.mleap.runtime.frame.Row.RowSelector
import ml.combust.mleap.runtime.function.{Selector, UserDefinedFunction}

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

/** Class for storing a leap frame locally in batches of rows.
  *
  * @param schema schema of leap frame
  */
case class BatchLeapFrame(override val schema: StructType,
                          dataset: Seq[Row]) extends LeapFrame[BatchLeapFrame] {
  def this(schema: StructType, rows: Iterable[Row]) = this(schema, rows.asScala.toSeq)

  /** Try to select fields to create a new LeapFrame.
    *
    * Returns a Failure if attempting to select any fields that don't exist.
    *
    * @param fieldNames field names to select
    * @return try new LeapFrame with selected fields
    */
  override def select(fieldNames: String*): Try[BatchLeapFrame] = {
    schema.indicesOf(fieldNames: _*).flatMap {
      indices =>
        schema.selectIndices(indices: _*).map {
          schema2 =>
            val dataset2 = dataset.map(_.selectIndices(indices: _*))
            BatchLeapFrame(schema2, dataset2)
        }
    }
  }

  /** Try to add a column to the LeapFrame.
    *
    * Returns a Failure if trying to add a field that already exists.
    *
    * @param name      name of column
    * @param selectors row selectors used to generate inputs to udf
    * @param udf       user defined function for calculating column value
    * @return LeapFrame with new column
    */
  override def withColumn(name: String, selectors: Selector*)
                         (udf: UserDefinedFunction): Try[BatchLeapFrame] = {
    val rowUDF : UserDefinedFunction = UserDefinedFunction(
      {(x:Row) => x},
      udf.output,
      udf.inputs
    )
    RowUtil.createRowSelectors(schema, selectors: _*)(udf).flatMap {
      rowSelectors =>
        val field = StructField(name, udf.outputTypes.head)
        schema.withField(field).map(schema2 => {
          val results = (udf.f.asInstanceOf[Seq[Row] => Seq[Row]])(dataset.map(r => udfValue(rowSelectors: _*)(rowUDF)(r)))
          val dataset2: Seq[Row] = dataset.zip(results).map {
            case (r1, r2) => r1.toSeq :+ r2.head
          }.map(x => Row(x: _*))
          BatchLeapFrame(schema2, dataset2)
        })
    }
  }

  /** Try to add multiple columns to the LeapFrame.
    *
    * Returns a Failure if trying to add a field that already exists.
    *
    * @param names     names of columns
    * @param selectors row selectors used to generate inputs to udf
    * @param udf       user defined function for calculating column values
    * @return LeapFrame with new columns
    */
  override def withColumns(names: Seq[String], selectors: Selector*)
                          (udf: UserDefinedFunction): Try[BatchLeapFrame] = {
    val rowUDF : UserDefinedFunction = UserDefinedFunction(
      {(x: Row) => x},
      udf.output,
      udf.inputs
    )
    RowUtil.createRowSelectors(schema, selectors: _*)(rowUDF).flatMap {
      rowSelectors =>
        val fields = names.zip(udf.outputTypes).map {
          case (name, dt) => StructField(name, dt)
        }

        schema.withFields(fields).map(
          schema2 => {
            val results = (udf.f.asInstanceOf[Seq[Row] => Seq[Row]])(dataset.map(r => udfValue(rowSelectors: _*)(rowUDF)(r)))
            val dataset2: Seq[Row] = dataset.zip(results).map {
              case (r1, r2) => r1.toSeq ++ r2.toSeq
            }.map(x => Row(x: _*))
            BatchLeapFrame(schema2, dataset2)
          })
    }
  }

  def udfValue(rowSelectors: RowSelector *)(udf : UserDefinedFunction)(row : Row): Row = {
    udf.inputs.length match {
      case 0 =>
        Row()
      case 1 =>
        Row(rowSelectors.head (row) )
      case 2 =>
        Row(rowSelectors.head (row), rowSelectors (1) (row) )
      case 3 =>
        Row(rowSelectors.head (row), rowSelectors (1) (row), rowSelectors (2) (row) )
      case 4 =>
        Row(rowSelectors.head (row), rowSelectors (1) (row), rowSelectors (2) (row), rowSelectors (3) (row) )
      case 5 =>
        Row(rowSelectors.head (row), rowSelectors (1) (row), rowSelectors (2) (row), rowSelectors (3) (row), rowSelectors (4) (row) )
    }
  }

  /** Try dropping column(s) from the LeapFrame.
    *
    * Returns a Failure if the column does not exist.
    *
    * @param names names of column to drop
    * @return LeapFrame with column(s) dropped
    */
  override def drop(names: String *): Try[BatchLeapFrame] = {
    for(indices <- schema.indicesOf(names: _*);
        schema2 <- schema.dropIndices(indices: _*)) yield {
      val dataset2 = dataset.map(_.dropIndices(indices: _*))
      BatchLeapFrame(schema = schema2, dataset = dataset2)
    }
  }

  /** Try filtering the leap frame using the UDF
    *
    * @param selectors row selectors used as inputs for the filter
    * @param udf       filter udf, must return a Boolean
    * @return LeapFrame with rows filtered
    */
  override def filter(selectors: Selector *)
                     (udf: UserDefinedFunction): Try[BatchLeapFrame] = {
    if(udf.outputTypes.length != 1 || udf.outputTypes.head.base != BasicType.Boolean) {
      return Failure(new IllegalArgumentException("must provide a UDF that outputs a boolean for filtering"))
    }

    RowUtil.createRowSelectors(schema, selectors: _*)(udf).map {
      rowSelectors =>
        val dataset2 = dataset.filter(_.shouldFilter(rowSelectors: _*)(udf))
        BatchLeapFrame(schema, dataset2)
    }
  }

  /** Collect all rows into a Seq
    *
    * @return all rows in the leap frame
    */
  override def collect(): Seq[Row] = dataset
}