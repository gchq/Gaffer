#!/bin/sh

./addElements.sh &

java -Dloader.path=/gaffer/jars/lib -jar jars/rest.jar
