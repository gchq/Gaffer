#!/usr/bin/env bash

set -e

currentYear=$(date +%Y)

for f in $(find . -type f -name '*.txt' -o -name '*.xml' -o -name '*.java' -o -name '*.js' -o -name '*.html' -o -name '*.sh' -o -name '*.cs' -o -name '*.py' -o -name '*.yml' -o -name '*.md' -o -name '*.yaml' -o -name '*.properties');
do
    echo ${f}
    for startYear in `seq 2016 1 $((currentYear - 1))`;
    do
        sedCmd="s/Copyright ${startYear} Crown Copyright/Copyright ${startYear}-${currentYear} Crown Copyright/g"
        #echo "sed -i'' -e "$sedCmd" ${f}"
        sed -i'' -e "$sedCmd" ${f}
        rm -f ${f}-e
        if [ $((startYear+1)) -lt ${currentYear} ]; then
            for endYear in `seq $((startYear + 1)) 1 $((currentYear - 1))`;
            do
                sedCmd="s/Copyright ${startYear}-${endYear} Crown Copyright/Copyright ${startYear}-${currentYear} Crown Copyright/g"
                #echo "sed -i'' -e "$sedCmd" ${f}"
                sed -i'' -e "$sedCmd" ${f}
                rm -f ${f}-e
            done
        fi
    done
done

