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

v_gaffer_jar_locn=/data/Gaffer
v_gaffer_tar_file=gaffer2.tar
v_accumulo_lib_locn=/opt/accumulo/lib/ext
v_files_in_org_lib=`ls -l /opt/accumulo/lib/ext/*.jar 2>/dev/null|wc -l`

if [ -d ${v_gaffer_jar_locn} ] && [ -e ${v_gaffer_jar_locn}/${v_gaffer_tar_file} ]; then
   tar xvf ${v_gaffer_jar_locn}/${v_gaffer_tar_file} -C ${v_accumulo_lib_locn}
   v_files_in_lib_after_copy=`ls -l /opt/accumulo/lib/ext/*.jar 2>/dev/null|wc -l`
   if [ ${v_files_in_lib_after_copy} > ${v_files_in_org_lib} ]; then
      echo "Gaffer setup completed"
   fi
fi
