import cml.data_v1 as cmldata

CONNECTION_NAME = "go01-aw-dl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Sample usage to run query through spark
EXAMPLE_SQL_QUERY = "show databases"
spark.sql(EXAMPLE_SQL_QUERY).show()

import os

from sedona.spark import *

config = SedonaContext.builder() .\
    config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.4.1,'
           'org.datasyslab:geotools-wrapper:1.4.0-28.2'). \
    getOrCreate()

sedona = SedonaContext.create(spark)

sc = sedona.sparkContext
sc.setSystemProperty("sedona.global.charset", "utf8")

point_csv_df = sedona.read.format("csv").\
    option("delimiter", ",").\
    option("header", "false").\
    load("data/testpoint.csv")

point_csv_df.createOrReplaceTempView("pointtable")

point_df = sedona.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
point_df.show(5)

countries = ShapefileReader.readToGeometryRDD(sc, "data/ne_50m_admin_0_countries_lakes/")
countries_df = Adapter.toDf(countries, sedona)
countries_df.createOrReplaceTempView("country")
countries_df.printSchema()

import os
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql.types import LongType, IntegerType, StringType
import dbldatagen as dg
import dbldatagen.distributions as dist

def points_gen(partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True):
    """
    Method to create dataframe with IoT readings and randomly located latitude and longitude
    -90 to 90 for latitude and -180 to 180 for longitude
    """

    manufacturers = ["IOT corp", "GEOSPATIAL Inc.", "United Geo Enterprises Ltd", "IOT_Century Corp", "Satellite Devices"]

    testDataSpec = (
        dg.DataGenerator(spark, name="iot", rows=row_count,partitions=partitions_num).withIdOutput()
        .withColumn("internal_device_id", "long", minValue=0x1000000000000,uniqueValues=unique_vals, omit=True, baseColumnType="hash",)
        .withColumn("device_id", "string", format="0x%013x", baseColumn="internal_device_id")
        .withColumn("manufacturer", "string", values=manufacturers, baseColumn="internal_device_id")
        .withColumn("model_ser", "integer", minValue=1, maxValue=11, baseColumn="device_id", baseColumnType="hash", omit=True, )
        .withColumn("event_type", "string", values=["activation", "deactivation", "plan change", "telecoms activity","internet activity", "device error"],random=True)
        .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00",end="2020-12-31 23:59:00",interval="1 minute", random=True )
        .withColumn("longitude", "float", minValue=-180, maxValue=180, random=True)
        .withColumn("latitude", "float", minValue=-90, maxValue=90, random=True)
    )

    df = testDataSpec.build()

    return df

iot_df = points_gen()

iot_df.createOrReplaceTempView("iottable")

iot_geo_df = sedona.sql("select id, device_id, manufacturer, event_type, event_ts, ST_Point(cast(iottable.latitude as Decimal(24,20)), cast(iottable.longitude as Decimal(24,20))) as arealandmark from iottable")

iot_geo_df.createOrReplaceTempView("iotgeotable")

result = sedona.sql("SELECT c.geometry as country_geom, c.NAME_EN, a.arealandmark as iot_device_location, a.device_id FROM country c, iotgeotable a WHERE ST_Contains(c.geometry, a.arealandmark)")

result.createOrReplaceTempView("result")
groupedresult = sedona.sql("SELECT c.NAME_EN, c.country_geom, count(*) as DeviceCount FROM result c GROUP BY c.NAME_EN, c.country_geom")
groupedresult.show()

"""DISTANCE JOIN"""

iot_geo_df_sample = spark.sql("SELECT * FROM iotgeotable LIMIT 10")
iot_geo_df_sample.createOrReplaceTempView("iot_geo_df_sample")


distance_join = """
                SELECT *
                FROM iot_geo_df_sample, iotgeotable
                WHERE ST_Distance(iot_geo_df_sample.arealandmark,iotgeotable.arealandmark) < 2
                """

spark.sql(distance_join).show()
print(iot_geo_df.count())
print(spark.sql(distance_join).count())
