#!/usr/bin/env bash


rm -rf temp
mkdir temp
cd temp
git clone https://github.com/wirktop/esutils
cd esutils
version=`cat pom.xml | grep -m 1 '<version>' | sed 's/.*<version>//; s/<\/version>.*//'`
mvn javadoc:javadoc
cd ../../

rm -rf $version
mkdir $version
cp -R temp/esutils/target/site/apidocs/* $version

rm -rf temp
