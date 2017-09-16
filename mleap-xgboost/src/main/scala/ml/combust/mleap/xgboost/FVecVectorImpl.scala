package ml.combust.mleap.xgboost

import biz.k11i.xgboost.util.FVec
import org.apache.spark.ml.linalg.Vector

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class FVecVectorImpl(vector: Vector) extends FVec {
  override def fvalue(index: Int): Double = vector(index)
}
