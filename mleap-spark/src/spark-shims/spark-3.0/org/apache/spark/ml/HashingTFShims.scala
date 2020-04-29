package org.apache.spark.ml

import org.apache.spark.ml.feature.HashingTF

object HashingTFShims {

  val runtimeVersion = 2

  def createHashingTF(uid: String, numFeatures: Int, binary: Boolean, version: Int): HashingTF = {
    require(version <= runtimeVersion, "Spark cannot load HashingTF transformer saved by higher version spark.")
    val m = new HashingTF(uid = uid).setNumFeatures(numFeatures).setBinary(binary)
    if (version < runtimeVersion) {
      throw new RuntimeException(
        "Cannot load HashingTF transformer created by lower version spark. Will fix this later.")
    }
    m
  }

}
