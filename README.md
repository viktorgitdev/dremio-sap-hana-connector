# Dremio SAP HANA ARP  Connector
<img src="https://www.dremio.com/img/dremio-website.png" width="60"> <img src="./src/main/resources/SAP-HANA.jpg" width="80">

## Overview
This is a community based SAP HANA Dremio connector made using the ARP framework. Check [Dremio Hub](https://github.com/dremio-hub) for more examples and [ARP Docs](https://github.com/dremio-hub/dremio-sqllite-connector#arp-file-format) for documentation.

## What is Dremio?

Dremio delivers lightning fast query speed and a self-service semantic layer operating directly against your data lake storage and other sources. No moving data to proprietary data warehouses or creating cubes, aggregation tables and BI extracts. Just flexibility and control for Data Architects, and self-service for Data Consumers.

## Usage

### Creating a new IBM SQL Query Source

### Required Parameters

Learn about IBM SQL Query JDBC client properties [here](https://cloud.ibm.com/docs/sql-query?topic=sql-query-jdbc)

- JDBC URL
  - Ex: `jdbc:ibmcloudsql:{instance-crn}?{targetcosurl}`
- API Key (IBM Cloud IAM API Key)

## Development

## Building and Installation

0. Change the pom's dremio.version to suit your Dremio's version.
   `<version.dremio> 4.9.1-202010230218060541-2e764ed0</version.dremio>`
1. In root directory with the pom.xml file run `mvn clean install -DskipTests`. If you want to run the tests, add the JDBC jar to your local maven repo along with environment variables that are required. Check the basic test example for more details.
1. Take the resulting .jar file in the target folder and put it in the <DREMIO_HOME>\jars folder in Dremio
2. Download the SAPA HANA JDBC driver from ([repo](https://mvnrepository.com/artifact/com.sap.cloud.db.jdbc/ngdbc)) and put in in the <DREMIO_HOME>\jars\3rdparty folder
3. Restart Dremio
