package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type ClientMessage struct {
	Tx_id    int64
	Hostname string
	Port     string
	Command  string
}

const ARG_NUM int = 2
const N int = 5 // num of nodes in the cluster

var node_name []string
var serv_addr []string
var serv_port []string
var enc *gob.Encoder
var wg sync.WaitGroup

func handle_err(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func handle_transaction() {
	var host string
	var port string
	var enc *gob.Encoder
	var dec *gob.Decoder
	var Tx_id int64

	reader := bufio.NewReader(os.Stdin)

	for {
		text, _ := reader.ReadString('\n')
		command := strings.Split(text[:len(text)-1], " ")
		if command[0] == "BEGIN" {
			// choose coordinator
			Tx_id = time.Now().UnixNano()

			coordIdx := rand.Intn(N)
			conn_tx, err := net.Dial("tcp", serv_addr[coordIdx]+":"+serv_port[coordIdx])
			handle_err(err)

			host, _, err = net.SplitHostPort(conn_tx.LocalAddr().String())
			handle_err(err)

			ln, err := net.Listen("tcp", host+":")
			handle_err(err)

			host, port, err = net.SplitHostPort(ln.Addr().String())
			handle_err(err)

			enc = gob.NewEncoder(conn_tx)
			enc.Encode(ClientMessage{Tx_id, host, port, command[0]})

			conn_rx, err := ln.Accept()
			handle_err(err)

			dec = gob.NewDecoder(conn_rx)

			var m string
			dec.Decode(&m)
			fmt.Println(m)

			defer ln.Close()
			break
		}
	}

	for {
		text, _ := reader.ReadString('\n')
		command := text[:len(text)-1]

		enc.Encode(ClientMessage{Tx_id, host, port, command})

		// wait for output
		var m string
		dec.Decode(&m)
		fmt.Println(m)

		if command == "COMMIT" || command == "ABORT" || m == "NOT FOUND, ABORTED" || m == "ABORTED" {
			wg.Done()
			return
		}
	}
}

func main() {
	argv := os.Args[1:]
	if len(argv) != ARG_NUM {
		fmt.Fprintf(os.Stderr, "usage: ./client <client id> <configuration file>\n")
		os.Exit(1)
	}

	config := argv[1]

	// read config file
	file, err := os.Open(config)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)

	node_name = make([]string, N)
	serv_addr = make([]string, N)
	serv_port = make([]string, N)

	for i := 0; i < 5; i++ {
		scanner.Scan()
		node_name[i] = scanner.Text()
		scanner.Scan()
		serv_addr[i] = scanner.Text()
		scanner.Scan()
		serv_port[i] = scanner.Text()
	}

	wg.Add(1)
	go handle_transaction()
	wg.Wait()
}
