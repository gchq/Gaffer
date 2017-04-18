#!/bin/bash

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
