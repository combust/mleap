#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaModel, JavaEstimator
from pyspark.ml.param.shared import *


class OneHotEncoder(JavaEstimator, HasInputCol, HasOutputCol, HasHandleInvalid, JavaMLReadable,
                    JavaMLWritable):

    dropLast = Param(Params._dummy(), "dropLast", "whether to drop the last category",
                     typeConverter=TypeConverters.toBoolean)

    def __init__(self, inputCol=None, outputCol=None):
        """
        __init__(self, includeFirst=True, inputCol=None, outputCol=None)
        """
        super(OneHotEncoder, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.mleap.feature.OneHotEncoder", self.uid)
        self._setDefault(dropLast=True)
        self.setInputCol(inputCol)
        self.setOutputCol(outputCol)

    def setParams(self,inputCol=None, outputCol=None):
        """
        setParams(self, dropLast=True, inputCol=None, outputCol=None)
        Sets params for this OneHotEncoder.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return OneHotEncoderModel(java_model)


class OneHotEncoderModel(JavaModel, JavaMLReadable, JavaMLWritable):
    def size(self):
        """
        Ordered list of labels, corresponding to indices to be assigned.
        """
        return self._call_java("size")
