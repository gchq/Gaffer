#!/bin/bash

# Copyright 2016 Crown Copyright
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

v_property_file=example/src/main/resources/example/films/mockaccumulostore.properties
v_property_bac_file=example/src/main/resources/example/films/mockaccumulostore.properties.bac

cp -pf ${v_property_file} ${v_property_bac_file}

sed 's#someInstanceName#Gaffer#' -i ${v_property_bac_file}
sed 's#aZookeeper#localhost:2181#' -i ${v_property_bac_file}
sed 's#gaffer.accumulostore.MockAccumuloStore#gaffer.accumulostore.AccumuloStore#' -i ${v_property_bac_file}
cp -pf ${v_property_bac_file} ${v_property_file}
rm -f ${v_property_bac_file}
cat ${v_property_file}
