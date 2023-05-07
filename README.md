# mongo-Spark

# 1. Create Virtual Machine
```
$ git clone https://github.com/developer-onizuka/scala
$ cd scale
$ vagrant up --provider=libvirt
$ vagrant ssh
```

# 2. Run mongoDB as a Container
```
$ sudo docker pull mongo:6.0.5
$ sudo docker run -d -p 27017:27017 --rm --name="mongodb" mongo:6.0.5
$ sudo docker exec -it mongodb /bin/bash
root@efe0e844a026:/#
```

# 3. Import csv file into mongodb instance
```
root@efe0e844a026:/# apt update
root@efe0e844a026:/# apt install -y git
root@efe0e844a026:/# git clone https://github.com/developer-onizuka/pandas
root@efe0e844a026:/# cd pandas
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

test> db.products.countDocuments()
295
```

# 4. Create SparkSession with MongoDB connection string
Please nothe my MongoDB works on 172.17.0.3 as a container as following.
```
$ sudo docker exec -it mongodb hostname -i
172.17.0.3
```
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

# 5. Extract from MongoDB
```
df = spark.read.format("mongo") \
               .option("database","test") \
               .option("collection","products") \
               .load()
```

# 6. Transform by Spark
```
from pyspark.sql.functions import desc

tmpdf = df.groupby(df['ModelName']).count().sort(desc("count"))
productcountdf = tmpdf.filter(tmpdf['count']>1)
productcountdf.show()
```

# 7. Load data onto MongoDB (to different collection)
```
productcountdf.write.format("mongo").mode("append") \
              .option("database","test2") \
              .option("collection","ModelNameCount") \
              .save()
```

# 8. BI Report
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
```
newdf = spark.read.parquet('./newdf')
newdf.show()
```

# Metastore in Apache Spark
> https://medium.com/@sarfarazhussain211/metastore-in-apache-spark-9286097180a4

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
        .enableHiveSupport() \
        .getOrCreate()
```

If you create the spark session without enableHiveSupport(), then the output of spark.sql.catalogImplementation must be None. Spark SQL defaults to in-memory (non-Hive) catalog unless you use spark-shell that does the opposite (uses Hive metastore).
```
conf = spark.sparkContext.getConf()
print("# spark.app.name = ", conf.get("spark.app.name"))
print("# spark.master = ", conf.get("spark.master"))
print("# spark.executor.memory = ", conf.get("spark.executor.memory"))
print("# spark.sql.warehouse.dir = ", conf.get("spark.sql.warehouse.dir"))
print("# spark.sql.catalogImplementation = ", conf.get("spark.sql.catalogImplementation"))

# spark.app.name =  myapp
# spark.master =  local
# spark.executor.memory =  1g
# spark.sql.warehouse.dir =  file:/home/jovyan/spark-warehouse
# spark.sql.catalogImplementation =  hive
```
```
df = spark.read.format("mongo") \
               .option("database","test") \
               .option("collection","products") \
               .load()
```

DataFrames can also be saved as persistent tables into Hive metastore using the saveAsTable command which will materialize the contents of the DataFrame and create a pointer to the data in the Hive metastore. 
```
df.write.mode("overwrite").saveAsTable("products_new")
```
```
%ls -l derby.log
-rw-r--r-- 1 jovyan users 672 May  7 05:10 derby.log
```
```
%cat derby.log
----------------------------------------------------------------
Sun May 07 05:10:06 UTC 2023:
Booting Derby version The Apache Software Foundation - Apache Derby - 10.14.2.0 - (1828579): instance a816c00e-0187-f49d-f849-0000042485f8 
on database directory /home/jovyan/metastore_db with class loader jdk.internal.loader.ClassLoaders$AppClassLoader@5ffd2b27 
Loaded from file:/usr/local/spark-3.2.1-bin-hadoop3.2/jars/derby-10.14.2.0.jar
java.vendor=Ubuntu
java.runtime.version=11.0.13+8-Ubuntu-0ubuntu1.20.04
user.dir=/home/jovyan
os.name=Linux
os.arch=amd64
os.version=5.4.0-139-generic
derby.system.home=null
Database Class Loader started - derby.database.classpath=''
```
```
spark.sql("SELECT * FROM products_new WHERE StandardCost > 2000").show()
+---------+--------+---------+---------+----------------+-------------+------------+-------------+--------------------+
|ListPrice|MakeFlag|ModelName|ProductID|     ProductName|ProductNumber|StandardCost|SubCategoryID|                 _id|
+---------+--------+---------+---------+----------------+-------------+------------+-------------+--------------------+
|  3578.27|       1| Road-150|      749|Road-150 Red, 62|   BK-R93R-62|   2171.2942|            2|{6456f3d06fcaf22f...|
|  3578.27|       1| Road-150|      750|Road-150 Red, 44|   BK-R93R-44|   2171.2942|            2|{6456f3d06fcaf22f...|
|  3578.27|       1| Road-150|      751|Road-150 Red, 48|   BK-R93R-48|   2171.2942|            2|{6456f3d06fcaf22f...|
|  3578.27|       1| Road-150|      753|Road-150 Red, 56|   BK-R93R-56|   2171.2942|            2|{6456f3d06fcaf22f...|
|  3578.27|       1| Road-150|      752|Road-150 Red, 52|   BK-R93R-52|   2171.2942|            2|{6456f3d06fcaf22f...|
+---------+--------+---------+---------+----------------+-------------+------------+-------------+--------------------+
```
