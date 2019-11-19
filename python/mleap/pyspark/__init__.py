import mleap.pyspark
import sys

sys.modules['pyspark.ml.mleap'] = mleap
sys.modules['pyspark.ml.mleap.pyspark'] = sys.modules['mleap.pyspark']
