#!/bin/bash

v_property_file=/Gaffer_Source/example/src/main/resources/store.properties
v_property_bac_file=/Gaffer_Source/example/src/main/resources/store.properties.bac

cp -pf ${v_property_file} ${v_property_bac_file}

sed 's#someInstanceName#Gaffer#' -i ${v_property_bac_file}
sed 's#aZookeeper#localhost#' -i ${v_property_bac_file}
cp -pf ${v_property_bac_file} ${v_property_file}
rm -f ${v_property_bac_file}
cat ${v_property_file}
