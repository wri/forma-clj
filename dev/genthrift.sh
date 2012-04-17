#!/bin/sh
rm -rf ../src/jvm/gen-java
rm -rf ../src/jvm/forma/schema
thrift -o "../src/jvm" -r --gen java:hashcode forma.thrift
mv ../src/jvm/gen-java/forma/schema ../src/jvm/forma
rm -rf ../src/jvm/gen-java
