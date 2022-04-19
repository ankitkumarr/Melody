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

# Second one joins the ring with the join address and id of the
# first server
$TIMEOUT ./main 22 8002 false 11 127.0.0.1:8001 &
pid[1]=$!
sleep 1

# Third one joins the ring with the join address and id of the
# second server
$TIMEOUT ./main 33 8003 false 22 127.0.0.1:8002 &
pid[2]=$!
sleep 1

trap "kill ${pid[0]} ${pid[1]} ${pid[2]}; exit 1" INT


###### TEST ######
# Send put to the first server
response=$(curl "localhost:8001/dhtput?key=foo&value=bar")
echo $response

if [[ $response != "Add key foo: bar to the DHT" ]] ; then
    echo "PUT failed"
    kill ${pid[0]} ${pid[1]} ${pid[2]}; exit 1
    exit 1
fi

# Attempt get from the third server
response=$(curl "localhost:8003/dhtget?key=foo")
echo response
if [[ $response != "Retrieved value for key foo: bar" ]] ; then
    echo "GET failed"
    kill ${pid[0]} ${pid[1]} ${pid[2]}; exit 1
    exit 1
fi

echo "TEST PASSED!"
echo "Press Ctrl C to exit..."
wait $pid