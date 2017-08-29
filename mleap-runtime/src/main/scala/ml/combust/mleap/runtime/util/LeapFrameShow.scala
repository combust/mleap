package ml.combust.mleap.runtime.util

import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.runtime.LeapFrame

/**
  * Created by hollinwilkins on 2/16/17.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/branch-2.1/sql/core/src/main/scala/org/apache/spark/sql/Dataset.scala#L246")
case class LeapFrameShow[LF <: LeapFrame[LF]](frame: LF, n: Int = 20, truncate: Int = 20) {
  override def toString: String = {
    val schema = frame.schema
    val dataset = frame.dataset.toLocal
    val rows = schema.fields.map(_.name) +: dataset.take(n).toSeq.map {
      _.map {
        cell =>
          val str = if (cell != null) cell match {
            case v: Option[_] => v.map(_.toString).getOrElse("null")
            case v: Seq[_] => v.mkString("[", ",", "]")
            case v => v.toString
          }else "null"

          if (truncate > 0 && str.length > truncate) {
            // do not show ellipses for strings shorter than 4 characters.
            if (truncate < 4) str.substring(0, truncate)
            else str.substring(0, truncate - 3) + "..."
          } else {
            str
          }
      }
    }

    val sb = new StringBuilder()
    val numCols = schema.fields.size

    // Initialise the width of each column to a minimum value of '3'
    val colWidths = Array.fill(numCols)(3)

    // Compute the width of each column
    for(row <- rows) {
      for((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }

    // Create SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

    // column names
    rows.head.zipWithIndex.map {
      case (cell, i) =>
        if (truncate > 0) {
          leftPad(cell, colWidths(i))
        } else {
          rightPad(cell, colWidths(i))
        }
    }.addString(sb, "|", "|", "|\n")

    sb.append(sep)

    // data
    rows.tail.map {
      _.zipWithIndex.map {
        case (cell, i) =>
          if (truncate > 0) {
            leftPad(cell.toString, colWidths(i))
          } else {
            rightPad(cell.toString, colWidths(i))
          }
      }.addString(sb, "|", "|", "|\n")
    }

    sb.append(sep)
    sb.toString
  }

  private def leftPad(str: String, padTo: Int, c: Char = ' '): String = {
    val n = padTo - str.length

    if(n > 0) {
      (0 until n).map(_ => c).mkString("") + str
    } else { str }
  }

  private def rightPad(str: String, padTo: Int, c: Char = ' '): String = {
    val n = padTo - str.length

    if(n > 0) {
      str + (0 until n).map(_ => c).mkString("")
    } else { str }
  }
}
