# Databricks notebook source
from pyspark.sql import functions as SF 

df = (spark.read.table('water_demos.schema_1.example_devices_edm')
.withColumn('Start',SF.to_timestamp(SF.col('Start'),'d/M/y H:m:s'))
.withColumn('End',SF.to_timestamp(SF.col('End'),'d/M/y H:m:s'))
)

df.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('water_demos.schema_1.example_devices_edm')

# COMMAND ----------

display(df)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE VIEW water_demos.schema_1.devices_all_intervals AS
# MAGIC SELECT DISTINCT
# MAGIC site_id,
# MAGIC `Start` AS time_interval
# MAGIC FROM
# MAGIC water_demos.schema_1.example_uu_devices_edm
# MAGIC WHERE site_id=1
# MAGIC UNION
# MAGIC SELECT DISTINCT
# MAGIC site_id,
# MAGIC `End` AS time_interval
# MAGIC FROM
# MAGIC water_demos.schema_1.example_uu_devices_edm
# MAGIC WHERE site_id=1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM water_demos.schema_1.devices_all_intervals

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW water_demos.schema_1.device_all_time_windows AS
# MAGIC SELECT
# MAGIC site_id,
# MAGIC time_interval AS start_time,
# MAGIC LEAD(time_interval, 1) OVER (PARTITION BY site_id ORDER BY time_interval) AS end_time
# MAGIC FROM
# MAGIC water_demos.schema_1.devices_all_intervals

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM water_demos.schema_1.device_all_time_windows

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC w.site_id,
# MAGIC w.start_time,
# MAGIC w.end_time,
# MAGIC SUM(CASE WHEN l.site_id IS NOT NULL THEN 1 ELSE 0 END) AS active_sensors
# MAGIC FROM
# MAGIC water_demos.schema_1.device_all_time_windows w
# MAGIC LEFT JOIN water_demos.schema_1.example_uu_devices_edm l
# MAGIC ON w.site_id = l.site_id
# MAGIC AND w.start_time < l.end
# MAGIC AND w.end_time >= l.start
# MAGIC GROUP BY
# MAGIC w.site_id,
# MAGIC w.start_time,
# MAGIC w.end_time

# COMMAND ----------

