#!/usr/bin/env bash

TIMEOUT="gtimeout -k 2s 20s "

cd ../src/main

# Build main
rm -f ./main
/usr/local/bin/go build main.go

# Start two servers
# First one starts the ring
$TIMEOUT ./main 11 8001 true &
pid[0]=$!
sleep 1

$TIMEOUT ./main 22 8002 false 11 127.0.0.1:8001 &
pid[1]=$!
sleep 1

id=33
port=8003
for (( i=2 ; i<10 ; i++ )) ; do
    $TIMEOUT ./main $id $port false 22 127.0.0.1:8002 &
    pid[$i]=$!
    id=$(($id + 11))
    port=$(($port + 1))
    sleep 1
done

kill_all () {
    for (( i=2 ; i<10 ; i++ )) ; do
        kill ${pid[$i]}
    done
    exit 1
}

trap "kill_all" INT


###### TEST ######
# Send put to the first server
response=$(curl "localhost:8001/dhtput?key=foo&value=bar")
echo $response

if [[ $response != "Add key foo: bar to the DHT" ]] ; then
    echo "PUT failed"
    kill_all
fi

# Attempt get from the third server
response=$(curl "localhost:8007/dhtget?key=foo")
echo $response
if [[ $response != "Retrieved value for key foo: bar" ]] ; then
    echo "GET failed"
    kill_all
    exit 1
fi

echo "TEST PASSED!"
rm -f ./main
kill_all
# echo "Press Ctrl C to exit..."
# wait $pid
