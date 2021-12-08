package ml.combust.mleap.core.types

import ml.combust.mleap.tensor.{ByteString, Tensor, DenseTensor, SparseTensor}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector => SparkVector}

import scala.util.{Failure, Success, Try}

/**
  * Created by hollinwilkins on 9/1/17.
  */
object Casting {
  val basicCast: Map[(BasicType, BasicType), (Any) => Any] = Map(
    (BasicType.Boolean, BasicType.Byte) -> { (v: Boolean) => (if (v) 1 else 0).toByte },
    (BasicType.Boolean, BasicType.Short) -> { (v: Boolean) => (if (v) 1 else 0).toShort },
    (BasicType.Boolean, BasicType.Int) -> { (v: Boolean) => if (v) 1 else 0 },
    (BasicType.Boolean, BasicType.Long) -> { (v: Boolean) => (if (v) 1 else 0).toLong },
    (BasicType.Boolean, BasicType.Float) -> { (v: Boolean) => (if (v) 1 else 0).toFloat },
    (BasicType.Boolean, BasicType.Double) -> { (v: Boolean) => (if (v) 1 else 0).toDouble },
    (BasicType.Boolean, BasicType.String) -> { (v: Boolean) => (if (v) 1 else 0).toString },

    (BasicType.Byte, BasicType.Boolean) -> { (v: Byte) => if (v != 0) true else false },
    (BasicType.Byte, BasicType.Short) -> { (v: Byte) => v.toShort },
    (BasicType.Byte, BasicType.Int) -> { (v: Byte) => v.toInt },
    (BasicType.Byte, BasicType.Long) -> { (v: Byte) => v.toLong },
    (BasicType.Byte, BasicType.Float) -> { (v: Byte) => v.toFloat },
    (BasicType.Byte, BasicType.Double) -> { (v: Byte) => v.toDouble },
    (BasicType.Byte, BasicType.String) -> { (v: Byte) => v.toString },

    (BasicType.Short, BasicType.Boolean) -> { (v: Short) => if (v != 0) true else false },
    (BasicType.Short, BasicType.Byte) -> { (v: Short) => v.toByte },
    (BasicType.Short, BasicType.Int) -> { (v: Short) => v.toInt },
    (BasicType.Short, BasicType.Long) -> { (v: Short) => v.toLong },
    (BasicType.Short, BasicType.Float) -> { (v: Short) => v.toFloat },
    (BasicType.Short, BasicType.Double) -> { (v: Short) => v.toDouble },
    (BasicType.Short, BasicType.String) -> { (v: Short) => v.toString },

    (BasicType.Int, BasicType.Boolean) -> { (v: Int) => if (v != 0) true else false },
    (BasicType.Int, BasicType.Byte) -> { (v: Int) => v.toByte },
    (BasicType.Int, BasicType.Short) -> { (v: Int) => v.toShort },
    (BasicType.Int, BasicType.Long) -> { (v: Int) => v.toLong },
    (BasicType.Int, BasicType.Float) -> { (v: Int) => v.toFloat },
    (BasicType.Int, BasicType.Double) -> { (v: Int) => v.toDouble },
    (BasicType.Int, BasicType.String) -> { (v: Int) => v.toString },

    (BasicType.Long, BasicType.Boolean) -> { (v: Long) => if (v != 0) true else false },
    (BasicType.Long, BasicType.Byte) -> { (v: Long) => v.toByte },
    (BasicType.Long, BasicType.Short) -> { (v: Long) => v.toShort },
    (BasicType.Long, BasicType.Int) -> { (v: Long) => v.toInt },
    (BasicType.Long, BasicType.Float) -> { (v: Long) => v.toFloat },
    (BasicType.Long, BasicType.Double) -> { (v: Long) => v.toDouble },
    (BasicType.Long, BasicType.String) -> { (v: Long) => v.toString },

    (BasicType.Float, BasicType.Boolean) -> { (v: Float) => if (v != 0.0) true else false },
    (BasicType.Float, BasicType.Byte) -> { (v: Float) => v.toByte },
    (BasicType.Float, BasicType.Short) -> { (v: Float) => v.toShort },
    (BasicType.Float, BasicType.Int) -> { (v: Float) => v.toInt },
    (BasicType.Float, BasicType.Long) -> { (v: Float) => v.toLong },
    (BasicType.Float, BasicType.Double) -> { (v: Float) => v.toDouble },
    (BasicType.Float, BasicType.String) -> { (v: Float) => v.toString },

    (BasicType.Double, BasicType.Boolean) -> { (v: Double) => if (v != 0.0) true else false },
    (BasicType.Double, BasicType.Byte) -> { (v: Double) => v.toByte },
    (BasicType.Double, BasicType.Short) -> { (v: Double) => v.toShort },
    (BasicType.Double, BasicType.Int) -> { (v: Double) => v.toInt },
    (BasicType.Double, BasicType.Long) -> { (v: Double) => v.toLong },
    (BasicType.Double, BasicType.Float) -> { (v: Double) => v.toFloat },
    (BasicType.Double, BasicType.String) -> { (v: Double) => v.toString },

    (BasicType.String, BasicType.Boolean) -> { (v: String) => v match {
      case "true" | "yes" => true
      case "false" | "no" => false
      case "" => false
      case digits if v.forall(_.isDigit) => digits.toDouble != 0
      case _ => true
    }
    },
    (BasicType.String, BasicType.Byte) -> { (v: String) => if (v == "null" || v == "") null else v.toByte },
    (BasicType.String, BasicType.Short) -> { (v: String) => if (v == "null" || v == "") null else v.toShort },
    (BasicType.String, BasicType.Int) -> { (v: String) => if (v == "null" || v == "") null else v.toInt },
    (BasicType.String, BasicType.Long) -> { (v: String) => if (v == "null" || v == "") null else v.toLong },
    (BasicType.String, BasicType.Float) -> { (v: String) => if (v == "null" || v == "") null else v.toFloat },
    (BasicType.String, BasicType.Double) -> { (v: String) => if (v == "null" || v == "") null else v.toDouble }
  ).map {
    case (k, v) => (k, v.asInstanceOf[(Any) => Any])
  }

  def sparkVectorToMLeapTensor(sparkVector: SparkVector): Tensor[Double] = {
    sparkVector match {
      case v: DenseVector => DenseTensor[Double](v.values, Seq(v.size))
      case v: SparseVector => SparseTensor[Double](v.indices.map(i => Seq(i)), v.values, Seq(v.size))
    }
  }

  def baseCast(from: BasicType, to: BasicType): Option[Try[(Any) => Any]] = {
    if(from == to) {
      None
    } else {
      Some {
        basicCast.get((from, to)) match {
          case Some(c) => Success(c)
          case None => Failure(new IllegalArgumentException(s"Cannot cast base type $from -> $to"))
        }
      }
    }
  }

  def cast(from: DataType, to: DataType): Option[Try[(Any) => Any]] = {
    val primaryCast = (from, to) match {
      case (_: ScalarType, _: ScalarType) =>
        baseCast(from.base, to.base)
      case (_: ScalarType, tt: TensorType) if tt.dimensions.exists(_.isEmpty) =>
        baseCast(from.base, to.base).map {
          _.flatMap {
            c =>
              Try {
                to.base match {
                  case BasicType.Boolean => (v: Any) => Tensor.scalar(c(v).asInstanceOf[Boolean])
                  case BasicType.Byte => (v: Any) => Tensor.scalar(c(v).asInstanceOf[Byte])
                  case BasicType.Short => (v: Any) => Tensor.scalar(c(v).asInstanceOf[Short])
                  case BasicType.Int => (v: Any) => Tensor.scalar(c(v).asInstanceOf[Int])
                  case BasicType.Long => (v: Any) => Tensor.scalar(c(v).asInstanceOf[Long])
                  case BasicType.Float => (v: Any) => Tensor.scalar(c(v).asInstanceOf[Float])
                  case BasicType.Double => (v: Any) => Tensor.scalar(c(v).asInstanceOf[Double])
                  case BasicType.String => (v: Any) => Tensor.scalar(c(v).asInstanceOf[String])
                  case BasicType.ByteString => (v: Any) => Tensor.scalar(c(v).asInstanceOf[ByteString])
                }
              }
          }
        }.orElse {
          Some {
            Try {
              from.base match {
                case BasicType.Boolean => (v: Any) => Tensor.scalar(v.asInstanceOf[Boolean])
                case BasicType.Byte => (v: Any) => Tensor.scalar(v.asInstanceOf[Byte])
                case BasicType.Short => (v: Any) => Tensor.scalar(v.asInstanceOf[Short])
                case BasicType.Int => (v: Any) => Tensor.scalar(v.asInstanceOf[Int])
                case BasicType.Long => (v: Any) => Tensor.scalar(v.asInstanceOf[Long])
                case BasicType.Float => (v: Any) => Tensor.scalar(v.asInstanceOf[Float])
                case BasicType.Double => (v: Any) => Tensor.scalar(v.asInstanceOf[Double])
                case BasicType.String => (v: Any) => Tensor.scalar(v.asInstanceOf[String])
                case BasicType.ByteString => (v: Any) => Tensor.scalar(v.asInstanceOf[ByteString])
              }
            }
          }
        }
      case (tt: TensorType, _: ScalarType) if tt.dimensions.exists(_.isEmpty) =>
        baseCast(from.base, to.base).map {
          _.map {
            c => (v: Any) => c(v.asInstanceOf[Tensor[_]](0))
          }
        }.orElse {
          Some {
            Try((v: Any) => v.asInstanceOf[Tensor[_]](0))
          }
        }
      case (_: ListType, _: ListType) =>
        baseCast(from.base, to.base).map {
          _.map {
            c => (l: Any) => l.asInstanceOf[Seq[Any]].map(c)
          }
        }
      case (_: ListType, _: TensorType) =>
        baseCast(from.base, to.base).map {
          _.flatMap {
            c => Try {
              to.base match {
                case BasicType.Boolean =>
                  val cc = c.asInstanceOf[(Any) => Boolean]
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(cc).toArray)
                case BasicType.Byte =>
                  val cc = c.asInstanceOf[(Any) => Byte]
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(cc).toArray)
                case BasicType.Short =>
                  val cc = c.asInstanceOf[(Any) => Short]
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(cc).toArray)
                case BasicType.Int =>
                  val cc = c.asInstanceOf[(Any) => Int]
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(cc).toArray)
                case BasicType.Long =>
                  val cc = c.asInstanceOf[(Any) => Long]
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(cc).toArray)
                case BasicType.Float =>
                  val cc = c.asInstanceOf[(Any) => Float]
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(cc).toArray)
                case BasicType.Double =>
                  val cc = c.asInstanceOf[(Any) => Double]
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(cc).toArray)
                case BasicType.String =>
                  val cc = c.asInstanceOf[(Any) => String]
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(cc).toArray)
                case BasicType.ByteString =>
                  val cc = c.asInstanceOf[(Any) => ByteString]
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(cc).toArray)
              }
            }
          }
        }.orElse {
          Some {
            Try {
              from.base match {
                case BasicType.Boolean =>
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(_.asInstanceOf[Boolean]).toArray)
                case BasicType.Byte =>
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(_.asInstanceOf[Byte]).toArray)
                case BasicType.Short =>
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(_.asInstanceOf[Short]).toArray)
                case BasicType.Int =>
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(_.asInstanceOf[Int]).toArray)
                case BasicType.Long =>
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(_.asInstanceOf[Long]).toArray)
                case BasicType.Float =>
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(_.asInstanceOf[Float]).toArray)
                case BasicType.Double =>
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(_.asInstanceOf[Double]).toArray)
                case BasicType.String =>
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(_.asInstanceOf[String]).toArray)
                case BasicType.ByteString =>
                  (v: Any) => Tensor.denseVector(v.asInstanceOf[Seq[_]].map(_.asInstanceOf[ByteString]).toArray)
              }
            }
          }
        }
      case (_: TensorType, _: ListType) =>
        baseCast(from.base, to.base).map {
          _.flatMap {
            c => Try {
              (v: Any) => {
                v match {
                  case _: SparseTensor[_] => throw new IllegalArgumentException("Cannot cast from SparseTensor to List")
                  case _: DenseTensor[_] => v.asInstanceOf[Tensor[_]].toArray.toList.map(c)
                  case _ => throw new IllegalArgumentException(s"${v.getClass.getName} is not a valid Tensor")
                }
              }
            }
          }
        }.orElse {
          Some {
            Try {
              (v: Any) => {
                v match {
                  case _: SparseTensor[_] => throw new IllegalArgumentException("Cannot cast from SparseTensor to List")
                  case _: DenseTensor[_] => v.asInstanceOf[Tensor[_]].toArray.toList
                  case _ => throw new IllegalArgumentException(s"${v.getClass.getName} is not a valid Tensor")
                }
              }
            }
          }
        }
      case (_: TensorType, _: TensorType) =>
        baseCast(from.base, to.base).map {
          _.map {
            c =>
              to.base match {
                case BasicType.Boolean =>
                  val cc = c.asInstanceOf[(Any) => Boolean]
                  (v: Any) => {
                    v match {
                      case _: SparkVector => sparkVectorToMLeapTensor(v.asInstanceOf[SparkVector]).mapValues(cc)
                      case _ => v.asInstanceOf[Tensor[_]].mapValues(cc)
                    }
                  }
                case BasicType.Byte =>
                  val cc = c.asInstanceOf[(Any) => Byte]
                  (v: Any) => {
                    v match {
                      case _: SparkVector => sparkVectorToMLeapTensor(v.asInstanceOf[SparkVector]).mapValues(cc)
                      case _ => v.asInstanceOf[Tensor[_]].mapValues(cc)
                    }
                  }
                case BasicType.Short =>
                  val cc = c.asInstanceOf[(Any) => Short]
                  (v: Any) => {
                    v match {
                      case _: SparkVector => sparkVectorToMLeapTensor(v.asInstanceOf[SparkVector]).mapValues(cc)
                      case _ => v.asInstanceOf[Tensor[_]].mapValues(cc)
                    }
                  }
                case BasicType.Int =>
                  val cc = c.asInstanceOf[(Any) => Int]
                  (v: Any) => {
                    v match {
                      case _: SparkVector => sparkVectorToMLeapTensor(v.asInstanceOf[SparkVector]).mapValues(cc)
                      case _ => v.asInstanceOf[Tensor[_]].mapValues(cc)
                    }
                  }
                case BasicType.Long =>
                  val cc = c.asInstanceOf[(Any) => Long]
                  (v: Any) => {
                    v match {
                      case _: SparkVector => sparkVectorToMLeapTensor(v.asInstanceOf[SparkVector]).mapValues(cc)
                      case _ => v.asInstanceOf[Tensor[_]].mapValues(cc)
                    }
                  }
                case BasicType.Float =>
                  val cc = c.asInstanceOf[(Any) => Float]
                  (v: Any) => {
                    v match {
                      case _: SparkVector => sparkVectorToMLeapTensor(v.asInstanceOf[SparkVector]).mapValues(cc)
                      case _ => v.asInstanceOf[Tensor[_]].mapValues(cc)
                    }
                  }
                case BasicType.Double =>
                  val cc = c.asInstanceOf[(Any) => Double]
                  (v: Any) => {
                    v match {
                      case _: SparkVector => sparkVectorToMLeapTensor(v.asInstanceOf[SparkVector]).mapValues(cc)
                      case _ => v.asInstanceOf[Tensor[_]].mapValues(cc)
                    }
                  }
                case BasicType.String =>
                  val cc = c.asInstanceOf[(Any) => String]
                  (v: Any) => {
                    v match {
                      case _: SparkVector => sparkVectorToMLeapTensor(v.asInstanceOf[SparkVector]).mapValues(cc)
                      case _ => v.asInstanceOf[Tensor[_]].mapValues(cc)
                    }
                  }
                case BasicType.ByteString =>
                  val cc = c.asInstanceOf[(Any) => ByteString]
                  (v: Any) => {
                    v match {
                      case _: SparkVector => sparkVectorToMLeapTensor(v.asInstanceOf[SparkVector]).mapValues(cc)
                      case _ => v.asInstanceOf[Tensor[_]].mapValues(cc)
                    }
                  }
              }
          }
        }.orElse {
          Some {
            Try {
              (v: Any) => {
                v match {
                  case _: SparkVector => sparkVectorToMLeapTensor(v.asInstanceOf[SparkVector])
                  case _ => v
                }
              }
            }
          }
        }
      case _ => Some(Failure(new IllegalArgumentException(s"Cannot cast $from to $to")))
    }

    (from.isNullable, to.isNullable) match {
      case (true, true) =>
        primaryCast.map {
          _.map {
            c =>
              // Handle nulls here
              (v: Any) => Option(v).map(c).orNull
          }
        }
      case (false, false) => primaryCast
      case (false, true) => primaryCast
      case (true, false) =>
        primaryCast.map {
          _.map {
            c =>
              // Handle nulls here
              (v: Any) =>
                c(Option(v).getOrElse {
                  throw new NullPointerException("trying to cast null to non-nullable value")
                })
          }
        }.orElse {
          Some {
            Try {
              // Handle nulls here
              (v: Any) =>
                Option(v).getOrElse {
                  throw new NullPointerException("trying to cast null to non-nullable value")
                }
            }
          }
        }
    }
  }
}
