package ml.combust.mleap.core.frame

import java.io.PrintStream

import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.core.function.Selector

import scala.util.Try

/**
  * Created by hollinwilkins on 10/5/17.
  */
trait TransformBuilder[LF <: TransformBuilder[LF]] {
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
  def select(fieldNames: String *): Try[LF]

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
                (udf: UserDefinedFunction): Try[LF]

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
                 (udf: UserDefinedFunction): Try[LF]

  /** Try dropping column(s) from the LeapFrame.
    *
    * Returns a Failure if the column does not exist.
    *
    * @param names names of columns to drop
    * @return LeapFrame with column(s) dropped
    */
  def drop(names: String *): Try[LF]

  /** Try filtering the leap frame using the UDF
    *
    * @param selectors row selectors used as inputs for the filter
    * @param udf filter udf, must return a Boolean
    * @return LeapFrame with rows filtered
    */
  def filter(selectors: Selector *)
            (udf: UserDefinedFunction): Try[LF]

  /** Print the schema to standard output.
    */
  def printSchema(): Unit = schema.print(System.out)

  /** Print the schema to a PrintStream.
    *
    * @param out print stream to print schema to
    */
  def printSchema(out: PrintStream): Unit = schema.print(out)
}
