#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import configparser
import random, os, sys
from datetime import datetime
from sedona.spark import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when
from keplergl import KeplerGl

## CDE PROPERTIES
config = configparser.ConfigParser()
config.read('/app/mount/jobCode/parameters.conf')
data_lake_name=config.get("general","data_lake_name")
username=config.get("general","username")

print("\nRunning as Username: ", username)

dbname = "CDE_DEMO_{}".format(username)

print("\nUsing DB Name: ", dbname)

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------

spark = SparkSession \
    .builder \
    .appName("IOT DEVICES LOAD") \
    .config("spark.kubernetes.access.hadoopFileSystems", data_lake_name)\
    .getOrCreate()

#---------------------------------------------------
#               CREATE SEDONA CONTEXT
#---------------------------------------------------

config = SedonaContext.builder().getOrCreate()

sedona = SedonaContext.create(spark)

sc = sedona.sparkContext
sc.setSystemProperty("sedona.global.charset", "utf8")

# Show catalog and database
print("SHOW CURRENT NAMESPACE")
spark.sql("SHOW CURRENT NAMESPACE").show()
spark.sql("USE {}".format(dbname))

# Show catalog and database
print("SHOW NEW NAMESPACE IN USE\n")
spark.sql("SHOW CURRENT NAMESPACE").show()

_DEBUG_ = False

#---------------------------------------------------
#                READ SOURCE TABLES
#---------------------------------------------------
print("JOB STARTED...")

#iot_geo_devices_df = spark.sql("SELECT * FROM {0}.IOT_GEO_DEVICES_{1}".format(dbname, username)) #could also checkpoint here but need to set checkpoint dir
#countries_geo_df = spark.sql("SELECT * FROM {0}.COUNTRIES_{1}".format(dbname, username))

airports = ShapefileReader.readToGeometryRDD(sc, "/app/mount/airportData")
airports_df = Adapter.toDf(airports, sedona)
airports_df.createOrReplaceTempView("airport")
airports_df.printSchema()

countries = ShapefileReader.readToGeometryRDD(sc, "/app/mount/countriesData")
countries_df = Adapter.toDf(countries, sedona)
countries_df.createOrReplaceTempView("country")
countries_df.printSchema()

airports_rdd = Adapter.toSpatialRdd(airports_df, "geometry")
# Drop the duplicate name column in countries_df
countries_df = countries_df.drop("NAME")
countries_rdd = Adapter.toSpatialRdd(countries_df, "geometry")

airports_rdd.analyze()
countries_rdd.analyze()

# 4 is the num partitions used in spatial partitioning. This is an optional parameter
airports_rdd.spatialPartitioning(GridType.KDBTREE, 4)
countries_rdd.spatialPartitioning(airports_rdd.getPartitioner())

buildOnSpatialPartitionedRDD = True
usingIndex = True
considerBoundaryIntersection = True
airports_rdd.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)

result_pair_rdd = JoinQueryRaw.SpatialJoinQueryFlat(airports_rdd, countries_rdd, usingIndex, considerBoundaryIntersection)

result2 = Adapter.toDf(result_pair_rdd, countries_rdd.fieldNames, airports.fieldNames, sedona)

result2.createOrReplaceTempView("join_result_with_all_cols")
# Select the columns needed in the join
result2 = sedona.sql("SELECT leftgeometry as country_geom, NAME_EN, rightgeometry as airport_geom, name FROM join_result_with_all_cols")

result2.show()
