# CDE_Geospatial_Article

### Steps

##### Custom Runtime Setup

docker build --network=host -t pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-sedona-geospatial-002 . -f Dockerfile

docker run -it docker build --network=host -t pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-sedona-geospatial-002 . -f Dockerfile /bin/bash

docker push pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-sedona-geospatial-002

##### Create CDE Docker Runtime for Sedona

cde credential create --name docker-creds-pauldefusco --type docker-basic --docker-server hub.docker.com --docker-username pauldefusco

cde resource create --name dex-spark-runtime-sedona-geospatial-pauldefusco --image pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-sedona-geospatial-002 --image-engine spark3 --type custom-runtime-image

##### Create CDE Docker Runtime for DBLDATAGEN

cde resource create --name dex-spark-runtime-dbldatagen-pauldefusco --image pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-dbldatagen-002 --image-engine spark3 --type custom-runtime-image

##### Create CDE Files Resource for Countries Data

cde resource create --name countries_data

##### Upload files to Countries Data Files Resource

cde resource upload --name countries_data --local-path ne_50m_admin_0_countries_lakes/ne_50m_admin_0_countries_lakes.cpg
cde resource upload --name countries_data --local-path ne_50m_admin_0_countries_lakes/ne_50m_admin_0_countries_lakes.dbf
cde resource upload --name countries_data --local-path ne_50m_admin_0_countries_lakes/ne_50m_admin_0_countries_lakes.prj
cde resource upload --name countries_data --local-path ne_50m_admin_0_countries_lakes/ne_50m_admin_0_countries_lakes.README.html
cde resource upload --name countries_data --local-path ne_50m_admin_0_countries_lakes/ne_50m_admin_0_countries_lakes.shp
cde resource upload --name countries_data --local-path ne_50m_admin_0_countries_lakes/ne_50m_admin_0_countries_lakes.shx
cde resource upload --name countries_data --local-path ne_50m_admin_0_countries_lakes/ne_50m_admin_0_countries_lakes.VERSION.txt

##### Create CDE Files Resource for Jars and Scripts

cde resource create --name sedona_dependencies

##### Upload JARS to CDE Files Resource

cde resource upload --name sedona_dependencies --local-path code/sedona-spark-common-3.0_2.12-1.5.0.jar
cde resource upload --name sedona_dependencies --local-path code/geotools-wrapper-1.5.0-28.2.jar
cde resource upload --name sedona_dependencies --local-path code/geospatial.py
cde resource upload --name sedona_dependencies --local-path code/staging_table.py
cde resource upload --name sedona_dependencies --local-path code/table_setup.py

##### Create CDE Jobs

cde job create --name tablesetup --type spark --runtime-image-resource-name dex-spark-runtime-sedona-geospatial-pauldefusco --jars geotools-wrapper-1.5.0-28.2.jar,sedona-spark-common-3.0_2.12-1.5.0.jar

cde job create --name staging_table --type spark --mount-1-resource sedona_dependencies --runtime-image-resource-name dex-spark-runtime-sedona-geospatial-pauldefusco --jars geotools-wrapper-1.5.0-28.2.jar,sedona-spark-common-3.0_2.12-1.5.0.jar

cde job create --name geospatial --type spark --mount-1-resource sedona_dependencies --runtime-image-resource-name dex-spark-runtime-sedona-geospatial-pauldefusco --jars geotools-wrapper-1.5.0-28.2.jar,sedona-spark-common-3.0_2.12-1.5.0.jar

##### Run CDE Jobs

cde job run --name tablesetup

cde job run --name staging_table

cde job run --name geospatial

##### Teardown

cde job create cleanup
