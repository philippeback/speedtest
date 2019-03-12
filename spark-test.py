import sys
import os
print("sys.path is:")
print(sys.path)

# Due to some bug, PySpark is not using the variables, so we are forced to make it pick the right python
os.environ["PYSPARK_PYTHON"] = "/opt/anaconda2/bin/python"
os.environ['PYTHONPATH'] = ':'.join(sys.path)

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.random import RandomRDDs
from time import time


print("########################################")
print("STARTING")
print("########################################")

sc = SparkContext(appName = "speedtest-nrb")
sql_context = SQLContext(sc)
start = time()

t = time()
print("creating dataframes df and df2")
df = RandomRDDs.uniformVectorRDD(sc, 100000000,2).map(lambda a : a.tolist()).toDF()
df2 = RandomRDDs.uniformVectorRDD(sc, 100000000,2).map(lambda a : a.tolist()).toDF()
print(time()-t)

t = time()
print("counting df")
df.count()
print(time()-t)

t = time()
print("counting df")
df.count()
print(time()-t)

t = time()
print("counting df")
df.count()
print(time()-t)

t = time()
print("counting df2")
df2.count()
print(time()-t)

t = time()
print("counting df2")
df2.count()
print(time()-t)

t = time()
print("counting df")
df.count()
print(time()-t)

t = time()
print("counting df")
df.count()
print(time()-t)

t = time()
print("df3 = df")
df3 = df
print(time()-t)

t = time()
print("df3 = df3.cache()")
df3 = df3.cache()
print(time()-t)

t = time()
print("counting df3")
df3.count()
print(time()-t)

t = time()
print("counting df3")
df3.count()
print(time()-t)

t = time()
print("counting df")
df.count()
print(time()-t)

t = time()
print("counting df3")
df3.count()
print(time()-t)

t = time()
print("counting df2")
df2.count()
print(time()-t)

t = time()
print("counting df3")
df3.count()
print(time()-t)


print("########################################")
print("DONE")
print("########################################")

end=time()
print("Total duration")
print(end-start)
