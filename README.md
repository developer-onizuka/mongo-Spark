# mongo-Spark

# 0. Create Virtual Machine
```
$ git clone https://github.com/developer-onizuka/scala
$ cd scale
$ vagrant up --provider=libvirt
$ vagrant ssh
```

# 1. Import csv file into mongodb instance
```
$ git clone http://github.com/developer-onziuka/pandas
$ cd pandas
```
```
root@efe0e844a026:/pandas# mongoimport --host="localhost" --port=27017 --db="test" --collection="products" --type="csv" --file="products.csv" --headerline
2023-03-20T07:30:24.991+0000	connected to: mongodb://localhost:27017/
2023-03-20T07:30:25.009+0000	295 document(s) imported successfully. 0 document(s) failed to import.
```
```
root@efe0e844a026:/pandas# mongosh

test> use test
already on db test

test> db.products.count()
DeprecationWarning: Collection.count() is deprecated. Use countDocuments or estimatedDocumentCount.
295
```

# 2. Create SparkSession with MongoDB connection string
Please nothe my MongoDB works on 172.17.0.3 as a container.
```
from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("myapp") \
        .master("local") \
        .config("spark.executor.memory", "1g") \
        .config("spark.mongodb.input.uri","mongodb://172.17.0.3:27017") \
        .config("spark.mongodb.output.uri","mongodb://172.17.0.3:27017") \
        .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
        .getOrCreate()
```

# 3. Extract from MongoDB
```
df = spark.read.format("mongo") \
               .option("database","test") \
               .option("collection","products") \
               .load()
```

# 4. Transform by Spark
```
from pyspark.sql.functions import desc

tmpdf = df.groupby(df['ModelName']).count().sort(desc("count"))
productcountdf = tmpdf.filter(tmpdf['count']>1)
productcountdf.show()
```

# 5. Load data onto MongoDB (to different collection)
```
productcountdf.write.format("mongo").mode("append") \
              .option("database","test2") \
              .option("collection","ModelNameCount") \
              .save()
```

# 6. BI Report
I used Jupyther notebook but Power BI might be a good solution with advantages over a \notebook for data visualization:<br>
- Power BI has advanced visualization capabilities
- Power BI provides secure distribution and sharing options for distribution of insights across an organization

```
dfPanda = productcountdf.toPandas()

from matplotlib import pyplot as plt

# Clear the plot area
plt.clf()

# Create a Figure
fig = plt.figure(figsize=(12,8))

# Create a bar plot of product counts by ModelName
plt.bar(x=dfPanda['ModelName'], height=dfPanda['count'], color='orange')

# Customize the chart
plt.title('Product Counts by ModelName greater than one')
plt.xlabel('ModelName')
plt.ylabel('Products')
plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
plt.xticks(rotation=70)

# Show the plot area
plt.show()
```
![mongo-Spark.png](https://github.com/developer-onizuka/mongo-Spark/blob/main/mongo-Spark.png)

# My Temporary Work
```
root@6f0287a605bd:/# cat <<EOF > test.json
{ "_id" : 1, "company" : "Tata", "quantity" : 10 }
{ "_id" : 2, "company" : "Mercedeze", "quantity" : 15 }
{ "_id" : 3, "company" : "Jeep", "quantity" : 20 }
EOF

root@6f0287a605bd:/# mongoimport --host="localhost" --port=27017 --db="test" --collection="cars" --type="json" --file="test.json"
2023-03-19T09:36:50.473+0000	connected to: mongodb://localhost:27017/
2023-03-19T09:36:50.489+0000	3 document(s) imported successfully. 0 document(s) failed to import.

root@6f0287a605bd:/# mongosh

test> show dbs
admin   40.00 KiB
config  12.00 KiB
local   40.00 KiB
test     8.00 KiB

test> use test
already on db test

test> show collections
cars

test> db.cars.find()
[
  { _id: 2, company: 'Mercedeze', quantity: 15 },
  { _id: 3, company: 'Jeep', quantity: 20 },
  { _id: 1, company: 'Tata', quantity: 10 }
]
```
```
test> db.cars.find({quantity : 15})
[ { _id: 2, company: 'Mercedeze', quantity: 15 } ]

test> db.cars.find({quantity :{$gte :15}})
[
  { _id: 2, company: 'Mercedeze', quantity: 15 },
  { _id: 3, company: 'Jeep', quantity: 20 }
]
```

# Store DataFrame as parquet format
```
df.write.mode("append").parquet("newdf")
```
```
(base) jovyan@d6fe5c01f3ed:~/mongo-Spark$ ls -l newdf/
total 16
-rw-r--r-- 1 jovyan users 13465 Mar 22 09:46 part-00000-a536a492-d71b-4c3e-acbe-eba382b7ebb6-c000.snappy.parquet
-rw-r--r-- 1 jovyan users     0 Mar 22 09:46 _SUCCESS
```

