#!/usr/bin/env bash


rm -rf temp
mkdir temp
cd temp
git clone https://github.com/wirktop/esutils
cd esutils
version=`cat pom.xml | grep -m 1 '<version>' | sed 's/.*<version>//; s/<\/version>.*//'`
mvn clean test javadoc:javadoc
cd ../../

rm -rf $version
mkdir $version

mkdir $version/apidocs
cp -R temp/esutils/target/site/apidocs $version
mkdir $version/code-coverage
cp -R temp/esutils/etc/code-coverage/jacoco-ut/* $version/code-coverage

rm -rf temp
