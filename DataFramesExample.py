from pyspark import SparkConf

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, IntegerType, StructField, StringType, TimestampType

orderSchema = StructType([
    StructField("order_id", IntegerType()),
    StructField("order_date", TimestampType()),
    StructField("order_customer_id", IntegerType()),
    StructField("order_status", StringType())
])

ordersDDL = "order_id Integer, order_date Timestamp, order_customer_id Integer, order_status String"

conf = SparkConf()
conf.set("spark.app.name", "My First App")
conf.set("spark.master", "local[2]")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

# orderDF: DataFrame = spark.read.format("csv").option("header", True).option("inferSchema", True).option("path",
#                                                                                                         "data/orders.csv").load()

# orderDF: DataFrame = spark.read.format("csv").option("header", True).option("schema", orderSchema).option("path",
#                                                                                                           "data/orders.csv").load()


orderDF: DataFrame = spark.read.format("csv").option("header", True).option("schema", ordersDDL).option("path",
                                                                                                        "data/orders.csv").load()

# orderDF.printSchema()

# orderDF.show()

# groupDF = orderDF.repartition(4) \
#     .where("order_customer_id > 10000") \
#     .select("order_id", "order_customer_id") \
#     .groupby("order_customer_id") \
#     .count()
#
# groupDF.show()

orderDF.printSchema()

spark.stop()
