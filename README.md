# mongo-Spark

```
root@6f0287a605bd:/# cat <<EOF > test.csv
> { "_id" : 1, "company" : "Tata", "quantity" : 10 }
{ "_id" : 2, "company" : "Mercedeze", "quantity" : 15 }
{ "_id" : 3, "company" : "Jeep", "quantity" : 20 }
> EOF

root@6f0287a605bd:/# mongoimport --host="localhost" --port=27017 --db="test_database" --collection="test_import" --type="json" --file="test.csv"
2023-03-19T09:36:50.473+0000	connected to: mongodb://localhost:27017/
2023-03-19T09:36:50.489+0000	3 document(s) imported successfully. 0 document(s) failed to import.

root@6f0287a605bd:/# mongosh

test> show dbs
admin          40.00 KiB
config         12.00 KiB
local          40.00 KiB
test_database  40.00 KiB

test> use test_database
switched to db test_database

test_database> show collections
test_import

test_database> db.test_import
test_database.test_import

test_database> db.test_import.find()
[
  { _id: 2, company: 'Mercedeze', quantity: 15 },
  { _id: 3, company: 'Jeep', quantity: 20 },
  { _id: 1, company: 'Tata', quantity: 10 }
]

```
