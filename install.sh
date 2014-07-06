#!/bin/sh

mvn install:install-file \
	-Dfile=dist/mongo-object-map-2014-04-30.jar \
	-DgroupId=com.thedeanda \
	-DartifactId=mongo-object-map \
	-Dversion=2014-04-30 \
	-Dpackaging=jar


