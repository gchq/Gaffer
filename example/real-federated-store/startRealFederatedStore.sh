#!/bin/bash

# Run this script from this repository.
# Usage: ./realFederatedStore.sh

SPRING_V=$(cat ../pom.xml | grep "^        <version>.*</version>$" | awk -F'[><]' '{print $3}')
SPRING_JAR=spring-rest-$SPRING_V-exec.jar
SPRING_TARGET=../../rest-api/spring-rest/target/$SPRING_JAR
PROP=federatedStore.properties
CONF=graphConfig.json
PROP_RESOURCE=../federated-demo/src/main/resources/$PROP
CONF_RESOURCE=../federated-demo/src/main/resources/$CONF

if [[ ! -f $CONF ]] 
then
	echo "Getting graph config $CONF_RESOURCE"
	cp $CONF_RESOURCE ./$CONF
fi

if [[ ! -f $PROP ]] 
then
	echo "Getting properties file: $PROP_RESOURCE"
	cp $PROP_RESOURCE ./$PROP
fi

if [[ ! -f $SPRING_JAR ]]
then
	echo "Getting Spring Jar $SPRING_TARGET"
	if [[ ! -f $SPRING_TARGET ]] 
	then
		echo "Spring Jar not found, so building project"
		mvn clean install -Pquick -f ../../
	fi 
	cp $SPRING_TARGET  ./$SPRING_JAR
	# echo "mvn clean"
	# mvn clean -f ../../
fi

java  -Dgaffer.storeProperties=$PROP -Dgaffer.graph.config=$CONF -jar $SPRING_JAR
