package ml.combust.mleap.xgboost.runtime.testing


trait FloatingPointApproximations {

  val DefaultMinimumPrecision = 1e-7

  def almostEqual(x: Double, y: Double, precision: Double = DefaultMinimumPrecision): Boolean = {
    if ((x - y).abs < precision) true else false
  }

  def almostEqualSequences(
                            sequenceA: Seq[Array[Double]],
                            SequenceB: Seq[Array[Double]],
                            precision: Double             = DefaultMinimumPrecision
                          ): Boolean = {
    for ((arrayA, arrayB) <- sequenceA zip SequenceB) {
      for ((numberA, numberB) <- arrayA zip arrayB) {
        if (!almostEqual(numberA, numberB, precision)) return false
      }
    }
    true
  }

}

