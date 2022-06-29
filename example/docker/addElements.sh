#!/bin/sh

if [[ ! -f my_file.json ]]
then
 exit 0
fi

until [ $(( ATTEMPTS++ )) -gt 60 ]; do
	result=$(wget -qO /dev/stdout 'http://localhost:8080/rest/graph/status')

	[ "${result}" == '{"status":"UP"}' ] && wget -qO /dev/stdout --header 'Content-Type: application/json' --post-data="$(cat ./my_file.json)" 'http://localhost:8080/rest/graph/operations/execute' && exit 0

	sleep 1
done

echo "ERROR: Cannot add data"
exit 1
