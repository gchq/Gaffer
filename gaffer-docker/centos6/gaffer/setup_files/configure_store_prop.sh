#!/bin/bash

v_property_file=example/src/main/resources/store.properties
v_property_bac_file=example/src/main/resources/store.properties.bac

cp -pf ${v_property_file} ${v_property_bac_file}

sed 's#someInstanceName#Gaffer#' -i ${v_property_bac_file}
sed 's#aZookeeper#localhost:2181#' -i ${v_property_bac_file}
sed 's#gaffer.accumulostore.MockAccumuloStore#gaffer.accumulostore.AccumuloStore#' -i ${v_property_bac_file}
cp -pf ${v_property_bac_file} ${v_property_file}
rm -f ${v_property_bac_file}
cat ${v_property_file}
