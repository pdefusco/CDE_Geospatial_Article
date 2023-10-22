# CDE Geospatial Article

### Objective

This git repository supports the Cloudera Community article on using Apache Sedona in Cloudera Data Engineering (CDE). You can run the following commands to set up the Sedona demo in your CDE Virtual Cluster.

Apache Sedona™ is a spatial computing engine that enables developers to easily process spatial data at any scale within modern cluster computing systems such as Apache Spark and Apache Flink. Sedona developers can express their spatial data processing tasks in Spatial SQL, Spatial Python or Spatial R.

CDP Data Engineering is the only cloud-native service purpose-built for enterprise data engineering teams. Building on Apache Spark, Data Engineering is an all-inclusive data engineering toolset that enables orchestration automation with Apache Airflow, advanced pipeline monitoring, visual troubleshooting, and comprehensive management tools to streamline ETL processes across enterprise analytics teams.

Data Engineering is fully integrated with Cloudera Data Platform, enabling end-to-end visibility and security with SDX as well as seamless integrations with CDP services such as Data Warehouse and Machine Learning. Data Engineering on CDP powers consistent, repeatable, and automated data engineering workflows on a hybrid cloud platform anywhere.

### Requirements

The following are required to reproduce the Demo in your CDE Virtual Cluster:

* CDE Service version 1.19 and above
* A Working installation of the CDE CLI. Instructions to install the CLI are provided [here](https://docs.cloudera.com/data-engineering/cloud/cli-access/topics/cde-cli.html).
* A working installation of git in your local machine. Please clone this git repository and keep in mind all commands assume they are run in the project's main directory.

##### Custom Runtime Setup

```
docker build --network=host -t pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-sedona-geospatial-002 . -f Dockerfile

docker run -it docker build --network=host -t pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-sedona-geospatial-002 . -f Dockerfile /bin/bash

docker push pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-sedona-geospatial-002
```

##### Create CDE Docker Runtime for Sedona

```
cde credential create --name docker-creds-pauldefusco --type docker-basic --docker-server hub.docker.com --docker-username pauldefusco

cde resource create --name dex-spark-runtime-sedona-geospatial-pauldefusco --image pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-sedona-geospatial-002 --image-engine spark3 --type custom-runtime-image
```

##### Create CDE Files Resource for Countries Data

```
cde resource create --name countries_data
```

##### Upload files to Countries Data Files Resource

```
cde resource upload-archive --name countries_data --local-path data/ne_50m_admin_0_countries_lakes.zip
```

##### Create CDE Files Resource for Jars and Scripts

```
cde resource create --name job_code
```

##### Upload JARS to CDE Files Resource

```
cde resource upload --name job_code --local-path code/geospatial.py --local-path code/staging_table.py --local-path code/table_setup.py --local-path code/parameters.conf --local-path code/utils.py
```

##### Create CDE Jobs

```
cde job create --name tablesetup --type spark --mount-1-prefix jobCode/ --mount-1-resource job_code --mount-2-prefix countriesData/ --mount-2-resource countries_data --runtime-image-resource-name dex-spark-runtime-sedona-geospatial-pauldefusco --packages org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.4.1,org.datasyslab:geotools-wrapper:1.4.0-28.2 --application-file jobCode/table_setup.py
```

```
cde job create --name staging_table --type spark --mount-1-prefix jobCode/ --mount-1-resource job_code --mount-2-prefix countriesData/ --mount-2-resource countries_data --runtime-image-resource-name dex-spark-runtime-sedona-geospatial-pauldefusco --packages org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.4.1,org.datasyslab:geotools-wrapper:1.4.0-28.2 --application-file jobCode/staging_table.py
```

```
cde job create --name geospatial --application-file jobCode/geospatial.py --type spark --mount-1-prefix jobCode/ --mount-1-resource job_code --runtime-image-resource-name dex-spark-runtime-sedona-geospatial-pauldefusco --packages org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.4.1,org.datasyslab:geotools-wrapper:1.4.0-28.2
```

##### Run CDE Jobs

```
cde job run --name tablesetup --executor-cores 2 --executor-memory "4g"

cde job run --name staging_table --executor-cores 2 --executor-memory "4g"

cde job run --name geospatial --executor-cores 2 --executor-memory "4g"
```
