#!/usr/bin/env bash

TIMEOUT="gtimeout -k 100s 100s "

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
for (( i=2 ; i<5 ; i++ )) ; do
    $TIMEOUT ./main $id $port false 22 127.0.0.1:8002 &
    pid[$i]=$!
    id=$(($id + 11))
    port=$(($port + 1))
    sleep 1
done

echo "Adding a key through some of the first nodes"
for (( i=0 ; i<5 ; i++ )) ; do
    add=$(($i + 8001))
    response=$(curl "localhost:$add/dhtput?key=$i&value=$i$i")
    if [[ $response != "Add key $i: $i$i to the DHT" ]] ; then
        echo "PUT failed"
        kill_all
    fi
done

for (( i=5 ; i<10 ; i++ )) ; do
    $TIMEOUT ./main $id $port false 22 127.0.0.1:8002 &
    pid[$i]=$!
    id=$(($id + 11))
    port=$(($port + 1))
    sleep 1
done

kill_all () {
    for (( i=0 ; i<10 ; i++ )) ; do
        kill ${pid[$i]}
    done
    exit 1
}

kill_all_success() {
    for (( i=0 ; i<10 ; i++ )) ; do
        kill ${pid[$i]}
    done
    exit 0
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

echo "BASIC TEST PASSED!"
echo "Now adding a key through every node (rest of the nodes)"
for (( i=5 ; i<10 ; i++ )) ; do
    add=$(($i + 8001))
    response=$(curl "localhost:$add/dhtput?key=$i&value=$i$i")
    if [[ $response != "Add key $i: $i$i to the DHT" ]] ; then
        echo "PUT failed"
        kill_all
    fi
done

echo "Getting the key in reverse order"
for (( i=0 ; i<10 ; i++ )) ; do
    add=$(($i + 8001))
    last=$((9 - $i))
    response=$(curl "localhost:$add/dhtget?key=$last")
    if [[ $response != "Retrieved value for key $last: $last$last" ]] ; then
        echo "GET failed"
        kill_all
    fi
done
echo "MULTIPLE KEYS IN ALL SERVERS TEST PASSED!"

echo "Now killing some servers"
kill_some () {
    for (( i=4 ; i<8 ; i++ )) ; do
        kill ${pid[$i]}
    done
    echo "TEST: Killed the servers"
}

kill_some

# Sleep a bit to give time for the ring to be auto-adjusted
sleep 8

echo "Attempting to retrieve some keys from remaining servers"
for (( i=0 ; i<4 ; i++ )) ; do
    add=$(($i + 8001))
    response=$(curl "localhost:$add/dhtget?key=$i")
    if [[ $response != "Retrieved value for key $i: $i$i" ]] ; then
        echo "GET failed"
        kill_all
    fi
    sleep 3
done

echo "PASSED!"
echo "Attempting to retrieve more keys"

ran=3
for (( i=8 ; i<10 ; i++ )) ; do
    add=$(($i + 8001))
    ran=$(($ran + 1))
    response=$(curl "localhost:$add/dhtget?key=$ran")
    if [[ $response != "Retrieved value for key $ran: $ran$ran" ]] ; then
        echo "GET failed"
        kill_all
    fi
done

echo "ALL TESTS PASSED!"
# wait ${pid[1]}
# wait ${pid[0]}
# wait ${pid[2]}
# wait ${pid[9]}
# wait ${pid[8]}

rm -f ./main
kill_all_success

# echo "Press Ctrl C to exit..."
# wait $pid
