# mongo-Spark

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
```
git clone http://github.com/developer-onziuka/pandas
cd pandas
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
