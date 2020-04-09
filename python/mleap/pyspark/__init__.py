import pyspark.ml
import mleap.pyspark
import mleap.pyspark.feature
from mleap.pyspark.feature.string_map import StringMap
import sys

sys.modules['pyspark.ml.mleap'] = mleap
sys.modules['pyspark.ml.mleap.pyspark'] = sys.modules['mleap.pyspark']
sys.modules['pyspark.ml.mleap.feature'] = sys.modules['mleap.pyspark.feature']

sys.modules['pyspark.ml'].mleap = mleap
sys.modules['pyspark.ml'].mleap.feature = sys.modules['mleap.pyspark.feature']
sys.modules['pyspark.ml'].mleap.feature.StringMap = StringMap
