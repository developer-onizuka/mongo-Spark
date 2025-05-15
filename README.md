# mongo-Spark

# コンテナを活用した MongoDB × Spark のデータ処理・分析

## **ゴール**
MongoDB と Apache Spark を組み合わせてデータ処理を行い、BIレポートを生成することが目的です。  
特に **コンテナ技術** を活用し、データの抽出・変換・ロード（ETL）を実施しながら分析を進めていきます。

---

## **ワークフロー概要**

### **1. 仮想マシンの作成**
Vagrant を使って仮想環境をセットアップし、開発環境を整える。

### **2. MongoDB をコンテナで実行**
Docker コンテナ上で MongoDB を起動し、データベースを運用する。

### **3. CSVデータを MongoDB にインポート**
`mongoimport` コマンドを使用して CSV ファイルを MongoDB に登録。

### **4. SparkSession の作成**
Spark の環境を構築し、MongoDB との接続を確立する。

### **5. MongoDB からデータを抽出**
Spark を使い MongoDB から `products` コレクションのデータを取得。

### **6. データの変換処理（ETL）**
Spark によるデータの **集計・フィルタリング** を行い、製品数が一定以上のデータを抽出。

### **7. 変換後のデータを MongoDB に保存**
処理結果を `test2` データベースの `ModelNameCount` コレクションへ保存。

### **8. BI レポートの作成**
Jupyter Notebook を用いてデータ可視化を行い、製品ごとのカウントをグラフで表示。  
**Power BI** を利用すると、安全なデータ共有や高度な可視化が可能。

### **9. 追加のデータ処理**
- JSON データを MongoDB にインポートし、検索・フィルタ処理を実施。
- DataFrame を **Parquet 形式** で保存し、後の分析に活用。

---


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
```
$ sudo docker run -it --rm -p 8888:8888 --name spark jupyter/all-spark-notebook:spark-3.2.0
```
Access the URL of http://127.0.0.1:8888/lab

Please note my MongoDB works on 172.17.0.2 as a container as following.
```
$ sudo docker exec -it mongodb hostname -i
172.17.0.2
```
```
from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("myapp") \
        .master("local") \
        .config("spark.executor.memory", "1g") \
        .config("spark.mongodb.input.uri","mongodb://172.17.0.2:27017") \
        .config("spark.mongodb.output.uri","mongodb://172.17.0.2:27017") \
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


