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

from pyspark.ml.wrapper import JavaTransformer
from pyspark.ml.base import Transformer
from pyspark.ml.util import _jvm


def serializeToBundle(self, path, dataset=None):
    serializer = SimpleSparkSerializer()
    serializer.serializeToBundle(self, path, dataset=dataset)


def deserializeFromBundle(path):
    serializer = SimpleSparkSerializer()
    return serializer.deserializeFromBundle(path)

setattr(Transformer, 'serializeToBundle', serializeToBundle)
setattr(Transformer, 'deserializeFromBundle', staticmethod(deserializeFromBundle))


class SimpleSparkSerializer(object):
    def __init__(self):
        super(SimpleSparkSerializer, self).__init__()
        self._java_obj = _jvm().ml.combust.mleap.spark.SimpleSparkSerializer()

    def serializeToBundle(self, transformer, path, dataset):
        self._java_obj.serializeToBundle(transformer._to_java(), path, dataset._jdf)

    def deserializeFromBundle(self, path):
        return JavaTransformer._from_java(self._java_obj.deserializeFromBundle(path))
