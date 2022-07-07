#!/bin/sh

if [[ ! -f addElements.json ]]
then
    echo "addElements.sh ERROR: addElements.json not found"
    exit 1
fi

sleep 30

until [ $(( ATTEMPTS++ )) -gt 60 ]; do
	result=$(wget -q -O /dev/stdout 'http://localhost:8080/rest/graph/status')

	[ "${result}" == '{"status":"UP"}' ] && wget -q -O /dev/stdout --header 'Content-Type: application/json' --post-data="$(cat ./addElements.json)" 'http://localhost:8080/rest/graph/operations/execute' && exit 0

	sleep 1
done

echo "addElements.sh ERROR: Cannot add data"
exit 1
