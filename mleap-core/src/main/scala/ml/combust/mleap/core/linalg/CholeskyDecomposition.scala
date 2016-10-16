/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ml.combust.mleap.core.linalg

import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import ml.combust.mleap.core.annotation.SparkCode
import org.netlib.util.intW

/**
  * Compute Cholesky decomposition.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/mllib/linalg/CholeskyDecomposition.scala")
object CholeskyDecomposition {

  /**
    * Solves a symmetric positive definite linear system via Cholesky factorization.
    * The input arguments are modified in-place to store the factorization and the solution.
    *
    * @param A the upper triangular part of A
    * @param bx right-hand side
    * @return the solution array
    */
  def solve(A: Array[Double], bx: Array[Double]): Array[Double] = {
    val k = bx.size
    val info = new intW(0)
    lapack.dppsv("U", k, 1, A, bx, k, info)
    val code = info.`val`
    assert(code == 0, s"lapack.dpotrs returned $code.")
    bx
  }
}
