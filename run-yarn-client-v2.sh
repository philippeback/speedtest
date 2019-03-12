PYSPARK_PYTHOB=/opt/anaconda2/bin/python \
spark-submit \
--master yarn \
--queue default \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/opt/anaconda2/bin/python \
--conf spark.yarn.appMasterEnv.PYTHONPATH=/usr/hdp/current/spark2-client/python/lib/py4j-0.10.4-src.zip:/usr/hdp/current/spark2-client/python:/opt/anaconda2/lib/python27.zip:/opt/anaconda2/lib/python2.7:/opt/anaconda2/lib/python2.7/plat-linux2:/opt/anaconda2/lib/python2.7/lib-tk:/opt/anaconda2/lib/python2.7/lib-old:/opt/anaconda2/lib/python2.7/lib-dynload:/opt/anaconda2/lib/python2.7/site-packages \
--conf spark.yarn.executorEnv.PYSPARK_PYTHON=/opt/anaconda2/bin/python \
--conf spark.yarn.executorEnv.PYTHONPATH=/usr/hdp/current/spark2-client/python/lib/py4j-0.10.4-src.zip:/usr/hdp/current/spark2-client/python:/opt/anaconda2/lib/python27.zip:/opt/anaconda2/lib/python2.7:/opt/anaconda2/lib/python2.7/plat-linux2:/opt/anaconda2/lib/python2.7/lib-tk:/opt/anaconda2/lib/python2.7/lib-old:/opt/anaconda2/lib/python2.7/lib-dynload:/opt/anaconda2/lib/python2.7/site-packages \
--conf spark.sql.shuffle.partitions=48 --conf spark.default.parallelism=48 \
--driver-java-options "-Dlog4j.configuration=file:./driver_log4j.properties" \
--num-executors 24 \
--executor-memory 4G \
--executor-cores 3 \
--driver-memory 2G \
spark-test.py
