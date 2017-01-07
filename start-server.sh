#!/usr/bin/env bash

# If timezone is not set to UTC, some time-based integration tests may fail
java -Duser.timezone=UTC -jar target/connector-server-cratedb-1.3.0-exec.jar
