# Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.

Sample Connector Server for building a crate.io connector from the Zoomdata 2.x Connector API

## Prerequisites

This project assumes working installations of:

* JDK 1.8
* Maven 3.x

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

## Limitations

This connector has only been tested with the version 0.55.4 of crate.io. It's not guaranteed to work with any other versions.

## Additional Notes

Although this implementation uses Java and some freely available libraries for convenience, they are not a *requirement* for 
building a connector server.

Any language capable of generating code from [Apache Thrift](https://thrift.apache.org/) can be used.