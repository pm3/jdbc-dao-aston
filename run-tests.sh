#!/bin/bash
export PATH="/home/pm/.sdkman/candidates/maven/current/bin:$PATH"
docker rm -f jdbc-dao-test-pg 2>/dev/null
mvn clean test "$@"
