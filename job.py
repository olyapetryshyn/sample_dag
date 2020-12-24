from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test JOB").getOrCreate()
rdd = spark.sparkContext.parallelize([1, 2])
print(sorted(rdd.cartesian(rdd).collect()))
