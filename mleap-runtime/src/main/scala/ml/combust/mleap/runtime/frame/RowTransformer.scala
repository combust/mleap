package ml.combust.mleap.runtime.frame

import ml.combust.mleap.core.types.{StructField, StructType}
import ml.combust.mleap.runtime.function.{Selector, UserDefinedFunction}

import scala.util.Try

/**
  * Created by hollinwilkins on 10/30/16.
  */
object RowTransformer {
  def apply(schema: StructType): RowTransformer = {
    RowTransformer(schema,
      schema,
      schema.fields.length,
      schema.fields.indices,
      List(),
      Seq())
  }
}

case class RowTransformer private (inputSchema: StructType,
                                   outputSchema: StructType,
                                   maxSize: Int,
                                   indices: Seq[Int],
                                   availableIndices: List[Int],
                                   transforms: Seq[(ArrayRow) => Option[ArrayRow]],
                                   shuffled: Boolean = false) extends FrameBuilder[RowTransformer] {
  override def schema: StructType = outputSchema

  override def select(fieldNames: String *): Try[RowTransformer] = {
    for (indices <- outputSchema.indicesOf(fieldNames: _*);
         schema2 <- outputSchema.selectIndices(indices: _*)) yield {
      val s = Set(indices: _*)
      val newIndices = indices.map(this.indices)
      val dropped = this.indices.zipWithIndex.filterNot {
        case (_, i) => s.contains(i)
      }.map(_._1)
      val newAvailableIndices = List(dropped: _*) ::: availableIndices

      copy(outputSchema = schema2,
        indices = newIndices,
        availableIndices = newAvailableIndices,
        shuffled = true)
    }
  }

  override def withColumn(name: String, selectors: Selector *)
                         (udf: UserDefinedFunction): Try[RowTransformer] = {
    ensureAvailableIndices(1).withColumnUnsafe(name, selectors: _*)(udf)
  }

  def withColumnUnsafe(name: String, selectors: Selector *)
                      (udf: UserDefinedFunction): Try[RowTransformer] = {
    val index :: newAvailableIndices = availableIndices

    IndexedRowUtil.createRowSelectors(outputSchema, this.indices, selectors: _*)(udf).flatMap {
      rowSelectors =>
        outputSchema.withField(name, udf.outputTypes.head).map {
          schema2 =>
            val transform = (row: ArrayRow) => Some(row.set(index, row.udfValue(rowSelectors: _*)(udf)))
            copy(outputSchema = schema2,
              indices = indices :+ index,
              availableIndices = newAvailableIndices,
              transforms = transforms :+ transform)
        }
    }
  }

  override def withColumns(names: Seq[String], selectors: Selector *)
                          (udf: UserDefinedFunction): Try[RowTransformer] = {
    ensureAvailableIndices(names.length).withColumnsUnsafe(names, selectors: _*)(udf)
  }

  def withColumnsUnsafe(names: Seq[String], selectors: Selector *)
                       (udf: UserDefinedFunction): Try[RowTransformer] = {
    val (indices, newAvailableIndices) = names.foldLeft((Seq[Int](), availableIndices)) {
      case ((is, ais), _) =>
        val i :: ai = ais
        (is :+ i, ai)
    }


    val fields = names.zip(udf.outputTypes).map {
      case (name, dt) => StructField(name, dt)
    }

    IndexedRowUtil.createRowSelectors(outputSchema, this.indices, selectors: _*)(udf).flatMap {
      rowSelectors =>
        outputSchema.withFields(fields).map {
          schema2 =>
            val transform = {
              (row: ArrayRow) =>
                val outputs = row.udfValue(rowSelectors: _*)(udf)
                val values = outputs match {
                  case r: ArrayRow => r.iterator.toSeq
                  case _ => outputs.asInstanceOf[Product].productIterator.toSeq
                }
                indices.zip(values).foreach {
                  case (index, value) => row.set(index, value)
                }

                Some(row)
            }

            copy(outputSchema = schema2,
              indices = this.indices ++ indices,
              availableIndices = newAvailableIndices,
              transforms = transforms :+ transform)
        }
    }
  }

  override def drop(names: String *): Try[RowTransformer] = {
    for (indices <- outputSchema.indicesOf(names: _*);
         schema2 <- outputSchema.dropIndices(indices: _*)) yield {
      val s = Set(indices: _*)
      val newIndices = this.indices.zipWithIndex.filterNot {
        case (_, i) => s.contains(i)
      }.map(_._1)
      val dropped = this.indices.zipWithIndex.filter {
        case (_, i) => s.contains(i)
      }.map(_._1)
      val newAvailableIndices = List(dropped: _*) ::: availableIndices

      copy(outputSchema = schema2,
        indices = newIndices,
        availableIndices = newAvailableIndices,
        shuffled = true)
    }
  }

  override def filter(selectors: Selector *)
                     (udf: UserDefinedFunction): Try[RowTransformer] = {
    IndexedRowUtil.createRowSelectors(outputSchema, indices, selectors: _*)(udf).map {
      rowSelectors =>
        val transform = (row: ArrayRow) => {
          if(row.shouldFilter(rowSelectors: _*)(udf)) {
            Some(row)
          } else { None }
        }

        copy(transforms = transforms :+ transform)
    }
  }

  def ensureAvailableIndices(numAvailable: Int): RowTransformer = {
    if(availableIndices.size < numAvailable) {
      val diff = numAvailable - availableIndices.size
      val newMaxSize = maxSize + diff
      val newAvailableIndices = availableIndices ::: List(maxSize until newMaxSize: _*)

      copy(maxSize = newMaxSize,
        availableIndices = newAvailableIndices)
    } else {
      this
    }
  }

  def transform(row: Row): Row = {
    transformOption(row).orNull
  }

  /** Transform an input row with the predetermined schema.
    *
    * @param row row to transform
    * @return transformed row, or None if filtered
    */
  def transformOption(row: Row): Option[ArrayRow] = {
    val arr = new Array[Any](maxSize)
    row.toArray.copyToArray(arr)
    val arrRow = ArrayRow(arr.toSeq)

    val r = transforms.foldLeft(Option(arrRow)) {
      (r, transform) => r.flatMap(transform)
    }

    if(shuffled) {
      r.map {
        row =>
          val values = indices.map(row.getRaw)
          ArrayRow(values)
      }
    } else {
      r
    }
  }
}
