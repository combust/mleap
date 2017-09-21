package ml.combust.mleap.xgboost

import biz.k11i.xgboost.util.FVec
import ml.combust.mleap.tensor.{SparseTensor, Tensor}

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class FVecTensorImpl(tensor: Tensor[Double]) extends FVec {
  assert(tensor.dimensions.size == 1, "must provide a vector")

  override def fvalue(index: Int): Double = tensor.get(index).getOrElse(Double.NaN)
}
