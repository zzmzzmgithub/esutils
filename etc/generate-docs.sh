#!/usr/bin/env bash

set -e
#mvn clean test javadoc:javadoc
version=`cat pom.xml | grep -m 1 '<version>' | sed 's/.*<version>//; s/<\/version>.*//'`

rm -rf temp
mkdir temp
git clone https://github.com/wirktop/esutils temp
cd temp && git checkout gh-pages && cd ..

rm -rf temp/$version
mkdir temp/$version
mkdir temp/$version/apidocs
mkdir temp/$version/code-coverage
cp -R target/site/apidocs temp/$version
cp -R etc/code-coverage/jacoco-ut/* temp/$version/code-coverage
rm -rf temp/current
mkdir temp/current
cp -R temp/$version/* temp/current

cd temp
git add -A
git commit -m "Docs for version $version"
git push
cd ..

rm -rf temp
