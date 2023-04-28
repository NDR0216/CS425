#!/bin/bash

code_folder='src/'
curr_folder=$(pwd)'/'
servers=(A B C D E)
ports=(10001 10002 10003 10004 10005)

# shutdown servers
shutdown() {
	for port in ${ports[@]}; do
		kill -15 $(lsof -ti:$port)
	done
}
trap shutdown EXIT

cd src
make
cd ..

# initialize servers
cp config.txt ${code_folder}/config.txt
cd $code_folder
for server in ${servers[@]}; do
	./server $server config.txt > ../server_${server}.log 2>&1 &
done

sleep 5

# run 2 tests
timeout -s SIGTERM 5s ./client a config.txt < ${curr_folder}input1.txt > ${curr_folder}output1.log 2>&1 &
timeout -s SIGTERM 5s ./client b config.txt < ${curr_folder}input2.txt > ${curr_folder}output2.log 2>&1 &
timeout -s SIGTERM 5s ./client c config.txt < ${curr_folder}input3.txt > ${curr_folder}output3.log 2>&1 &
timeout -s SIGTERM 5s ./client d config.txt < ${curr_folder}input4.txt > ${curr_folder}output4.log 2>&1 &
timeout -s SIGTERM 5s ./client e config.txt < ${curr_folder}input5.txt > ${curr_folder}output5.log 2>&1 &

sleep 5

cd $curr_folder
echo "Difference between your output and expected output:"
diff output1.log expected1.txt
diff output2.log expected2.txt
