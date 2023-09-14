import data
import prop as prop

from pyspark.sql import *




spark = SparkSession.builder.appName('table').config("spark.jars","C:\Installers\Driver\postgresql-42.6.0.jar").getOrCreate()



properties = {

"driver": "org.postgresql.Driver",
"user": "postgres",
"password": "1613"

}


url = "jdbc:postgresql://localhost:5432/postgres"



# reading data files


customers = spark.read.jdbc(url=url, table='customers', properties=properties)

items = spark.read.jdbc(url=url, table='items', properties=properties)

orders = spark.read.jdbc(url=url, table='orders', properties=properties)

order_details = spark.read.jdbc(url=url, table='order_details', properties=properties)

salesperson = spark.read.jdbc(url=url, table='salesperson', properties=properties)

ship_to = spark.read.jdbc(url=url, table='ship_to', properties=properties)

#customers.show()
customers.write.format("parquet").mode("append").save("C:/parquet/customers/")
items.write.format("parquet").mode("append").save("C:/parquet/items/")
orders.write.format("parquet").mode("append").save("C:/parquet/orders/")
order_details.write.format("parquet").mode("append").save("C:/parquet/order_details/")
salesperson.write.format("parquet").mode("append").save("C:/parquet/salesperson/")
ship_to.write.format("parquet").mode("append").save("C:/parquet/ship_to/")


tables = [customers,items,orders,order_details,salesperson,ship_to]




for table in tables:
   print(table.show())
