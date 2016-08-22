package ml.combust.mleap.runtime.transformer.builder

import ml.combust.mleap.runtime.types.{DataType, StructField}
import ml.combust.mleap.runtime.{LeapFrame, Row}

import scala.util.{Failure, Success, Try}

/**
  * Created by hwilkins on 11/15/15.
  */
case class LeapFrameBuilder[F <: LeapFrame[F]](frame: F)
  extends TransformBuilder[LeapFrameBuilder[F]] with Serializable {
  override def withInput(name: String): Try[(LeapFrameBuilder[F], Int)] = {
    frame.schema.indexOf(name).map((this, _))
  }

  override def withInput(name: String, dataType: DataType): Try[(LeapFrameBuilder[F], Int)] = {
    frame.schema.indexedField(name).flatMap {
      case (index, field) =>
        if(field.dataType.fits(dataType)) {
          Success(this, index)
        } else {
          Failure(new Error(s"Field $name expected data type ${field.dataType} but found $dataType"))
        }
    }
  }

  override def withOutput(name: String, dataType: DataType)(o: (Row) => Any): Try[LeapFrameBuilder[F]] = {
    frame.withField(name, dataType)(o).map {
      frame2 => copy(frame = frame2)
    }
  }

  override def withOutputs(fields: Seq[StructField])(o: (Row) => Row): Try[LeapFrameBuilder[F]] = {
    frame.withFields(fields)(o).map {
      frame2 => copy(frame = frame2)
    }
  }
}
