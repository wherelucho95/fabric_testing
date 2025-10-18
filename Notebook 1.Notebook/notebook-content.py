# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "af58ae68-80e5-40e1-9b7b-383b30bf3eb0",
# META       "default_lakehouse_name": "Raw_data",
# META       "default_lakehouse_workspace_id": "7368e6d6-9481-4cef-9fd6-3bd07c6b4e77",
# META       "known_lakehouses": [
# META         {
# META           "id": "af58ae68-80e5-40e1-9b7b-383b30bf3eb0"
# META         },
# META         {
# META           "id": "b4bd1bc2-adbb-43b4-882b-b87a03698a32"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Raw_data.Salary_Data LIMIT 1000")
display(df)
print(type(df))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/testing/health_lifestyle_dataset.csv")
# df now is a Spark DataFrame containing CSV data from "Files/testing/health_lifestyle_dataset.csv".
display(type(df))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Crear DataFrame
data = [
    ("O001", "Ana", "Electrónica", 2, 1200.50),
    ("O002", "Luis", "Ropa", 5, 250.00),
    ("O003", "Carla", "Electrónica", 1, 799.99),
    ("O004", "Pedro", "Hogar", 3, 450.00),
    ("O005", "Ana", "Ropa", 2, 180.00)
]

# Asignarle columnas

columns = ["order_id","customer","category","quantity","price"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Nuevo_df = spark.createDataFrame(data,schema=columns)
display(Nuevo_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#path = "Tables/EJEMPLO2"
Nuevo_df.write.mode("overwrite").format("delta").saveAsTable("EJEMPLO2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select *
# MAGIC from ejemplo

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select *
# MAGIC from names

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_1 = spark.sql("select * from ejemplo")
display(df_1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_1 = spark.sql('''
select * 
from ejemplo
''')
display(df_1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_1 = spark.sql('''
select * 
from ejemplo
''').createOrReplaceTempView("df_1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from df_1

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_2 = spark.read.table("ejemplo")

#df_2 = df_2.select("order_id","customer")

display(df_2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_1 = spark.sql('''
select *
from ejemplo
where category = 'Ropa'
''')
display(df_1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_2_filtrado = df_2.filter(df_2['category'] == 'Ropa')
display(df_2_filtrado)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_1 = spark.sql('''
select *,
quantity * price as total
from ejemplo
''')
display(df_1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_2_addcolumn =  df_2.withColumn("total", df_2.quantity *  df_2.price)

display(df_2_addcolumn)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_1 = spark.sql('''
select customer, category,
sum(quantity)
from ejemplo
group by customer, category
''')
display(df_1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_2_agrupado = df_2.groupBy("customer","category").sum("quantity")
display(df_2_agrupado)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_1 = spark.sql('''
select customer, category,
sum(quantity) as total_quantity
from ejemplo
group by customer, category
order by total_quantity DESC
''')
display(df_1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#from pyspark.sql.functions import col

df_2_agrupado = (df_2.groupBy("customer","category")
                .sum("quantity")
                .withColumnRenamed("sum(quantity)","total_quantity")
                .orderBy("total_quantity",ascending = False))
display(df_2_agrupado)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
