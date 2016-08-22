package org.apache.spark.ml.mleap.feature

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.mleap.param.HasDropLast
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Dataset, functions}
import org.apache.spark.sql.types.{DoubleType, NumericType, StructType}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

trait OneHotEncoderBase extends HasInputCol with HasOutputCol with HasDropLast

class OneHotEncoderModel(override val uid: String, val size: Int) extends Model[OneHotEncoderModel]
  with OneHotEncoderBase {
  def this(size: Int) = this(Identifiable.randomUID("oneHotEncoderModel"), size)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def copy(extra: ParamMap): OneHotEncoderModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)
    val shouldDropLast = $(dropLast)
    val oneValue = Array(1.0)
    val emptyValues = Array[Double]()
    val emptyIndices = Array[Int]()
    val encode = functions.udf { label: Double =>
      if (label < size) {
        Vectors.sparse(size, Array(label.toInt), oneValue)
      } else {
        Vectors.sparse(size, emptyIndices, emptyValues)
      }
    }

    var outputAttrGroup = AttributeGroup.fromStructField(
      transformSchema(dataset.schema)(outputColName))
    if (outputAttrGroup.size < 0) {
      val outputAttrNames = Array.tabulate(size)(_.toString)
      val filtered = if (shouldDropLast) outputAttrNames.dropRight(1) else outputAttrNames
      val outputAttrs: Array[Attribute] =
        filtered.map(name => BinaryAttribute.defaultAttr.withName(name))
      outputAttrGroup = new AttributeGroup(outputColName, outputAttrs)
    }
    val metadata = outputAttrGroup.toMetadata()

    dataset.select(dataset.col("*"), encode(dataset.col(inputColName).cast(DoubleType)).as(outputColName, metadata))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)

    require(schema(inputColName).dataType.isInstanceOf[NumericType],
      s"Input column must be of type NumericType but got ${schema(inputColName).dataType}")
    val inputFields = schema.fields
    require(!inputFields.exists(_.name == outputColName),
      s"Output column $outputColName already exists.")

    val inputAttr = Attribute.fromStructField(schema(inputColName))
    val outputAttrNames: Option[Array[String]] = inputAttr match {
      case nominal: NominalAttribute =>
        if (nominal.values.isDefined) {
          nominal.values
        } else if (nominal.numValues.isDefined) {
          nominal.numValues.map(n => Array.tabulate(n)(_.toString))
        } else {
          None
        }
      case binary: BinaryAttribute =>
        if (binary.values.isDefined) {
          binary.values
        } else {
          Some(Array.tabulate(2)(_.toString))
        }
      case _: NumericAttribute =>
        throw new RuntimeException(
          s"The input column $inputColName cannot be numeric.")
      case _ =>
        None // optimistic about unknown attributes
    }

    val filteredOutputAttrNames = outputAttrNames.map { names =>
      if ($(dropLast)) {
        require(names.length > 1,
          s"The input column $inputColName should have at least two distinct values.")
        names.dropRight(1)
      } else {
        names
      }
    }

    val outputAttrGroup = if (filteredOutputAttrNames.isDefined) {
      val attrs: Array[Attribute] = filteredOutputAttrNames.get.map { name =>
        BinaryAttribute.defaultAttr.withName(name)
      }
      new AttributeGroup($(outputCol), attrs)
    } else {
      new AttributeGroup($(outputCol))
    }

    val outputFields = inputFields :+ outputAttrGroup.toStructField()
    StructType(outputFields)
  }
}

class OneHotEncoder(override val uid: String) extends Estimator[OneHotEncoderModel]
  with OneHotEncoderBase {

  /** @group setParam */
  def setDropLast(value: Boolean): this.type = set(dropLast, value)
  setDefault(dropLast -> true)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: Dataset[_]): OneHotEncoderModel = {
    // schema transformation
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)
    val shouldDropLast = $(dropLast)
    var outputAttrGroup = AttributeGroup.fromStructField(
      transformSchema(dataset.schema)(outputColName))
    if (outputAttrGroup.size < 0) {
      // If the number of attributes is unknown, we check the values from the input column.
      val numAttrs = dataset.select(dataset.col(inputColName).cast(DoubleType)).rdd.map(_.getDouble(0))
        .aggregate(0.0)(
          (m, x) => {
            assert(x <= Int.MaxValue,
              s"OneHotEncoder only supports up to ${Int.MaxValue} indices, but got $x")
            assert(x >= 0.0 && x == x.toInt,
              s"Values from column $inputColName must be indices, but got $x.")
            math.max(m, x)
          },
          (m0, m1) => {
            math.max(m0, m1)
          }
        ).toInt + 1

      val outputAttrNames = Array.tabulate(numAttrs)(_.toString)
      val filtered = if (shouldDropLast) outputAttrNames.dropRight(1) else outputAttrNames
      val outputAttrs: Array[Attribute] =
        filtered.map(name => BinaryAttribute.defaultAttr.withName(name))
      outputAttrGroup = new AttributeGroup(outputColName, outputAttrs)
    }

    val size = outputAttrGroup.size

    copyValues(new OneHotEncoderModel(uid, size).setParent(this))
  }

  override def copy(extra: ParamMap): Estimator[OneHotEncoderModel] = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)

    require(schema(inputColName).dataType.isInstanceOf[NumericType],
      s"Input column must be of type NumericType but got ${schema(inputColName).dataType}")
    val inputFields = schema.fields
    require(!inputFields.exists(_.name == outputColName),
      s"Output column $outputColName already exists.")

    val inputAttr = Attribute.fromStructField(schema(inputColName))
    val outputAttrNames: Option[Array[String]] = inputAttr match {
      case nominal: NominalAttribute =>
        if (nominal.values.isDefined) {
          nominal.values
        } else if (nominal.numValues.isDefined) {
          nominal.numValues.map(n => Array.tabulate(n)(_.toString))
        } else {
          None
        }
      case binary: BinaryAttribute =>
        if (binary.values.isDefined) {
          binary.values
        } else {
          Some(Array.tabulate(2)(_.toString))
        }
      case _: NumericAttribute =>
        throw new RuntimeException(
          s"The input column $inputColName cannot be numeric.")
      case _ =>
        None // optimistic about unknown attributes
    }

    val filteredOutputAttrNames = outputAttrNames.map { names =>
      if ($(dropLast)) {
        require(names.length > 1,
          s"The input column $inputColName should have at least two distinct values.")
        names.dropRight(1)
      } else {
        names
      }
    }

    val outputAttrGroup = if (filteredOutputAttrNames.isDefined) {
      val attrs: Array[Attribute] = filteredOutputAttrNames.get.map { name =>
        BinaryAttribute.defaultAttr.withName(name)
      }
      new AttributeGroup($(outputCol), attrs)
    } else {
      new AttributeGroup($(outputCol))
    }

    val outputFields = inputFields :+ outputAttrGroup.toStructField()
    StructType(outputFields)
  }
}
