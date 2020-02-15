package ml.combust.mleap.xgboost.runtime.struct

import biz.k11i.xgboost.util.FVec
import ml.combust.mleap.tensor.Tensor


/**
  * Created by hollinwilkins on 9/16/17.
  */
case class FVecTensorImpl(tensor: Tensor[Double]) extends FVec {
  assert(tensor.dimensions.size == 1, "must provide a vector")

  // Casting to floats, because doubles result in compounding differences from the c++ implementation
  // https://github.com/komiya-atsushi/xgboost-predictor-java/issues/21
  override def fvalue(index: Int): Double = tensor.get(index).getOrElse(Double.NaN).toFloat
}
