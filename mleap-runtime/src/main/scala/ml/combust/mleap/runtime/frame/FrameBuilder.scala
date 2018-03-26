package ml.combust.mleap.runtime.frame

import java.io.PrintStream

import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.function.{Selector, UserDefinedFunction}

import scala.util.Try

/**
  * Created by hollinwilkins on 10/5/17.
  */
trait FrameBuilder[FB <: FrameBuilder[FB]] {
  /** Get the schema.
    *
    * @return schema
    */
  def schema: StructType

  /** Try to select fields to create a new LeapFrame.
    *
    * Returns a Failure if attempting to select any fields that don't exist.
    *
    * @param fieldNames field names to select
    * @return try new LeapFrame with selected fields
    */
  def select(fieldNames: String *): Try[FB]

  /** Selects all of the fields from field names that exist in the leap frame.
    * Returns a new leap frame with all of the available fields.
    *
    * @param fieldNames fields to try and select
    * @return leap frame with select fields
    */
  def relaxedSelect(fieldNames: String *): FB = {
    val actualFieldNames = fieldNames.filter(schema.hasField)
    select(actualFieldNames: _*).get
  }

  /** Try to add a column to the LeapFrame.
    *
    * Returns a Failure if trying to add a field that already exists.
    *
    * @param name name of column
    * @param selectors row selectors used to generate inputs to udf
    * @param udf user defined function for calculating column value
    * @return LeapFrame with new column
    */
  def withColumn(name: String, selectors: Selector *)
                (udf: UserDefinedFunction): Try[FB]

  @deprecated("this method will be removed for version 1.0, use #withColumn", "MLeap 0.9.0")
  def withField(name: String, selectors: Selector *)
               (udf: UserDefinedFunction): Try[FB] = withColumn(name, selectors: _*)(udf)

  @deprecated("this method will be removed for version 1.0, use #withColumn", "MLeap 0.9.0")
  def withOutput(name: String, selectors: Selector *)
                (udf: UserDefinedFunction): Try[FB] = withColumn(name, selectors: _*)(udf)

  /** Try to add multiple columns to the LeapFrame.
    *
    * Returns a Failure if trying to add a field that already exists.
    *
    * @param names names of columns
    * @param selectors row selectors used to generate inputs to udf
    * @param udf user defined function for calculating column values
    * @return LeapFrame with new columns
    */
  def withColumns(names: Seq[String], selectors: Selector *)
                 (udf: UserDefinedFunction): Try[FB]

  @deprecated("this method will be removed for version 1.0, use #withColumns", "MLeap 0.9.0")
  def withFields(names: Seq[String], selectors: Selector *)
                (udf: UserDefinedFunction): Try[FB] = withColumns(names, selectors: _*)(udf)


  @deprecated("this method will be removed for version 1.0, use #withColumns", "MLeap 0.9.0")
  def withOutputs(names: Seq[String], selectors: Selector *)
                 (udf: UserDefinedFunction): Try[FB] = withColumns(names, selectors: _*)(udf)

  /** Try dropping column(s) from the LeapFrame.
    *
    * Returns a Failure if the column does not exist.
    *
    * @param names names of columns to drop
    * @return LeapFrame with column(s) dropped
    */
  def drop(names: String *): Try[FB]

  @deprecated("this method will be removed for version 1.0, use #drop", "MLeap 0.9.0")
  def dropField(name: String): Try[FB] = drop(name)

  /** Try filtering the leap frame using the UDF
    *
    * @param selectors row selectors used as inputs for the filter
    * @param udf filter udf, must return a Boolean
    * @return LeapFrame with rows filtered
    */
  def filter(selectors: Selector *)
            (udf: UserDefinedFunction): Try[FB]

  /** Print the schema to standard output.
    */
  def printSchema(): Unit = schema.print(System.out)

  /** Print the schema to a PrintStream.
    *
    * @param out print stream to print schema to
    */
  def printSchema(out: PrintStream): Unit = schema.print(out)
}
