# Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.

Sample Connector Server for building a crate.io connector from the Zoomdata 2.x Connector API

## Prerequisites

This project assumes working installations of:

* JDK 1.8
* Maven 3.x
* Docker (only required for connector testing)

## <a name="starting"></a>Starting the Connector Server

To build the project:

`mvn clean install`

To run the server:

`./start-server.sh`

By default, the connector will serve connections over HTTP at http://localhost:7337/connector

## Connecting to Zoomdata

Refer to the [official Zoomdata docs](http://docs.zoomdata.com/managing-connector-services) for instructions to register a connector server.

Once the connector server is created, the connection type will need to be manually added.

Set the Storage Type to `CRATEDB`.

Add the following connection parameters:

    Parameter Name      Type       Required

    JDBC_URL            Text        Yes
    USER_NAME           Text        No
    PASSWORD            Password    No

There's also a [tutorial video](https://drive.google.com/open?id=0B5hqni4_xCGadGVMek40SDcyTVU) that can walk you through the process.

## Testing the Connector

_TODO: Link the Zoomdata connector testing guide when it's made publicly available_

This package also includes a simple crate.io container pre-loaded with the Zoomdata connector testing reference data. It uses [Docker](https://www.docker.com/) to run the container so no further installation is required.

First, build the container with:

`docker build -t zoomdata/crate-test test-server`

Run the container exposing the default ports. The ports can be adjusted if needed:

`docker run -it --rm -p 4200:4200 -p 4300:4300 -p 5432:5432 zoomdata/crate-test`

Assuming the default ports were used, you should now be able to see the two testing tables in the `integration_tests` schema via the web admin UI:

http://localhost:4200/_plugin/crate-admin/#/tables

It may take a moment for the data to replicate and show as available.

With our sample data source ready, [start the connector server](#starting) and launch `connector-shell` as provided by the Zoomdata testing guide.

In connector shell, create a data source (assuming default ports):

`datasource add -n cratedb CONNECTOR_TYPE CRATEDB JDBC_URL crate://localhost:5432/`

Run the structured query test suite to validate structured request and query functionality:

`test -ds cratedb -u structured -s integration_tests -c connector_test`

Run the meta test suite to validate server description and meta functionality:

`test -ds cratedb -u meta -s integration_tests -c meta_test`

## Limitations

The connector uses the CrateDB JDBC driver 2.x, which means that it is compatible with CrateDB >= 0.57.0. The driver does not use the CrateDB transport port to connect to a CrateDB server any longer and the connection string format has changed since the driver version 1.x.

Please see the [CrateDB JDBC documentation](https://crate.io/docs/reference/jdbc/en/2.1.6/#jdbc-driver-class) for further reference.

## Additional Notes

Although this implementation uses Java and some freely available libraries for convenience, they are not a *requirement* for building a connector server.

Any language capable of generating code from [Apache Thrift](https://thrift.apache.org/) can be used.
