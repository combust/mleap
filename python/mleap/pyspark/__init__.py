import sys

import pyspark.ml

import mleap.pyspark
import mleap.pyspark.feature

sys.modules['pyspark.ml.mleap'] = mleap
sys.modules['pyspark.ml.mleap.pyspark'] = sys.modules['mleap.pyspark']
sys.modules['pyspark.ml.mleap.feature'] = sys.modules['mleap.pyspark.feature']
