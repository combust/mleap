package org.apache.spark.ml

import org.apache.spark.mllib.tree.impurity.ImpurityCalculator

object TreeShims {

  def getImpurityCalculator(impurity: String, stats: Array[Double]): ImpurityCalculator = {
    ImpurityCalculator.getCalculator(impurity, stats)
  }

}
