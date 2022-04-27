from pyspark import SparkContext, StorageLevel
from sys import stdin

sc = SparkContext("local[*]", "PremiumCustomers")

base_rdd = sc.textFile("data/customerorders.csv")

mapped_input = base_rdd.map(lambda x: (x.split(",")[0], float(x.split(",")[2])))

total_by_customer = mapped_input.reduceByKey(lambda x, y: x + y)

premium_customers = total_by_customer.filter(lambda x: x[1] > 5000)

doubled_amt = premium_customers.map(lambda x: (x[0], x[1] * 2)).persist(StorageLevel.MEMORY_ONLY)

result = doubled_amt.collect()

for x in result:
    print(x)

print(doubled_amt.count())

# stdin.readline()