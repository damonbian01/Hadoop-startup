#!/bin/bash

cd ../
echo "package now ..."
mvn clean package -Dmaven.test.skip=true
