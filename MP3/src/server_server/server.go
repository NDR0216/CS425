package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ServerMessage struct {
	Tx_id    int64
	Coord_id string
	Command  string
	Result   string
}

type ClientMessage struct {
	Tx_id    int64
	Hostname string
	Port     string
	Command  string
}

type balance struct {
	cond           *sync.Cond
	value          int // committed value
	Tx_id          int64
	readTimestamp  map[int64]bool // list of tx id
	tentativeWrite map[int64]int  // list of tentative writes, [(tx1, value1), ...]
}

const ARG_NUM int = 2
const N int = 5 // num of nodes in the cluster

var id string // current node
var serverEncMap map[string]*gob.Encoder
var clientEncMap map[int64]*gob.Encoder
var mu_clientEncMap sync.Mutex
var balanceMap map[string]*balance // per-object state
var mu_balanceMap sync.Mutex
var prepareReplyMap map[int64][]string // counting how many reply for each tx
var mu_prepareMap sync.Mutex
var Tx_id_Map map[string]int64
var wg sync.WaitGroup

func handle_err(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		// os.Exit(1)
	}
}

func print_balance(balance_copy map[string]int) {
	for k, v := range balance_copy {
		if v > 0 {
			fmt.Print(k, " = ", v, ", ")
		}
	}
	fmt.Print("\n")
}

func read(Tx_id int64, account string) (int, bool, bool) {
	// read value, non empty, non aborted

	mu_balanceMap.Lock()

	_, ok := balanceMap[account]
	if !ok { // does not exist
		// create a empty object
		m := &sync.Mutex{}

		newBalance := &balance{
			cond:           sync.NewCond(m),
			readTimestamp:  make(map[int64]bool),
			tentativeWrite: make(map[int64]int)}

		newBalance.readTimestamp[Tx_id] = true // read empty object still counts!!!

		balanceMap[account] = newBalance

		mu_balanceMap.Unlock()
		return 0, false, true // NOT FOUND, ABORTED
	}

	mu_balanceMap.Unlock()

	// if object exist
	balanceMap[account].cond.L.Lock()
	for {
		if Tx_id > balanceMap[account].Tx_id {
			value_Ds := balanceMap[account].value
			Tx_id_Ds := balanceMap[account].Tx_id

			for t, v := range balanceMap[account].tentativeWrite {
				if t <= Tx_id && t > Tx_id_Ds { // maximum write timestamp <= Tc
					value_Ds = v
					Tx_id_Ds = t
				}
			}

			if Tx_id_Ds == balanceMap[account].Tx_id { // Ds is committed
				if value_Ds == 0 && Tx_id_Ds == 0 { // empty object
					balanceMap[account].readTimestamp[Tx_id] = true

					balanceMap[account].cond.L.Unlock()
					return 0, false, true // NOT FOUND, ABORTED
				}

				value := value_Ds                               // read Ds
				balanceMap[account].readTimestamp[Tx_id] = true // add Tc to RTS list

				balanceMap[account].cond.L.Unlock()
				return value, true, true
			} else if Tx_id_Ds == Tx_id { // if Ds was written by Tc, simply read Ds
				value := value_Ds // read temporary write

				balanceMap[account].cond.L.Unlock()
				return value, true, true
			} else {
				// wait
				balanceMap[account].cond.Wait()
				// try again
			}
		} else { // too late
			balanceMap[account].cond.L.Unlock()

			return 0, false, false // ABORTED
		}
	}
}

func write(Tx_id int64, account string, value int) bool {
	balanceMap[account].cond.L.Lock()

	var maxRTS int64
	for k := range balanceMap[account].readTimestamp {
		if k > maxRTS {
			maxRTS = k
		}
	}

	if Tx_id >= maxRTS && Tx_id > balanceMap[account].Tx_id {
		balanceMap[account].tentativeWrite[Tx_id] = value

		balanceMap[account].cond.L.Unlock()
		return true
	} else { // too late
		balanceMap[account].cond.L.Unlock()
		return false // NOT FOUND, ABORTED
	}
}

func prepare(Tx_id int64) bool { // there would only be one TW accualy, so I ignore the commit rule
	mu_balanceMap.Lock()
	keys := make([]string, len(balanceMap))

	i := 0
	for k := range balanceMap {
		keys[i] = k
		i++
	}
	mu_balanceMap.Unlock()

	// check if negative
	for _, k := range keys {
		balanceMap[k].cond.L.Lock()

		elem, ok := balanceMap[k].tentativeWrite[Tx_id]

		if ok && elem < 0 {
			balanceMap[k].cond.L.Unlock()
			return false
		}
		balanceMap[k].cond.L.Unlock()
	}

	return true
}

func commit(Tx_id int64, commit bool) { // there would only be one TW accualy, so I ignore the commit rule
	mu_balanceMap.Lock()
	keys := make([]string, len(balanceMap))

	i := 0
	for k := range balanceMap {
		keys[i] = k
		i++
	}
	mu_balanceMap.Unlock()

	print := false
	balance_copy := make(map[string]int)

	for _, k := range keys {
		balanceMap[k].cond.L.Lock()

		elem, ok := balanceMap[k].tentativeWrite[Tx_id]

		if ok {
			print = true

			balanceMap[k].value = elem
			balanceMap[k].Tx_id = Tx_id
		}

		delete(balanceMap[k].tentativeWrite, Tx_id)

		balance_copy[k] = balanceMap[k].value

		balanceMap[k].cond.L.Unlock()
		balanceMap[k].cond.Broadcast()
	}

	if print {
		print_balance(balance_copy)
	}
}

func abort(Tx_id int64, commit bool) {
	mu_balanceMap.Lock()
	keys := make([]string, len(balanceMap))

	i := 0
	for k := range balanceMap {
		keys[i] = k
		i++
	}
	mu_balanceMap.Unlock()

	for _, k := range keys {
		balanceMap[k].cond.L.Lock()
		delete(balanceMap[k].readTimestamp, Tx_id)
		delete(balanceMap[k].tentativeWrite, Tx_id)

		balanceMap[k].cond.L.Unlock()
		balanceMap[k].cond.Broadcast()
	}
}

func handle_command(Tx_id int64, command string) string {
	arg := strings.Split(command, " ")
	// arg[0] = command
	// arg[1] = server.account
	// arg[2] = amount

	switch arg[0] {
	case "DEPOSIT":
		amount, err := strconv.Atoi(arg[2])
		handle_err(err)

		value, _, suscess := read(Tx_id, arg[1])

		if !suscess {
			return "ABORTED"
		}

		suscess = write(Tx_id, arg[1], value+amount)

		if suscess {
			return "OK"
		} else {
			return "ABORTED"
		}
	case "BALANCE":
		value, nonEmpty, suscess := read(Tx_id, arg[1])

		if !suscess {
			return "ABORTED"
		}

		if nonEmpty {
			return arg[1] + " = " + strconv.Itoa(value)
		} else {
			return "NOT FOUND, ABORTED"
		}
	case "WITHDRAW":
		amount, err := strconv.Atoi(arg[2])
		handle_err(err)

		value, nonEmpty, suscess := read(Tx_id, arg[1])

		if !suscess {
			return "ABORTED"
		}

		if nonEmpty {
			suscess = write(Tx_id, arg[1], value-amount)
			if suscess {
				return "OK"
			} else {
				return "ABORTED"
			}
		} else {
			return "NOT FOUND, ABORTED"
		}
	case "PREPARE":
		ok := prepare(Tx_id)

		if ok {
			return "Yes"
		} else {
			return "No"
		}
	case "COMMIT":
		commit(Tx_id, true)

		return "COMMIT OK"
	case "ABORT":
		abort(Tx_id, false)

		return "ABORTED"
	}

	return ""
}

func handle_server_msg(m ServerMessage) {
	if m.Coord_id == id { // reply from other branch
		if m.Command == "PREPARE" {
			mu_prepareMap.Lock()
			prepareReplyMap[m.Tx_id] = append(prepareReplyMap[m.Tx_id], m.Result)

			if len(prepareReplyMap[m.Tx_id]) < N {
				mu_prepareMap.Unlock()
				return
			}

			if len(prepareReplyMap[m.Tx_id]) == N {
				commitOrAbort := "COMMIT"

				for _, s := range prepareReplyMap[m.Tx_id] {
					if s == "No" {
						commitOrAbort = "ABORT"
						break
					}
				}

				mu_prepareMap.Unlock()

				for k := range serverEncMap {
					err := serverEncMap[k].Encode(ServerMessage{
						Tx_id:    m.Tx_id,
						Coord_id: id,
						Command:  commitOrAbort}) // forward the command to the correct branch
					handle_err(err)
				}

				result := handle_command(m.Tx_id, commitOrAbort)

				mu_clientEncMap.Lock()
				err := clientEncMap[m.Tx_id].Encode(result) // output to the client
				mu_clientEncMap.Unlock()
				handle_err(err)
			}
		} else {
			mu_clientEncMap.Lock()
			err := clientEncMap[m.Tx_id].Encode(m.Result) // output to the client
			mu_clientEncMap.Unlock()
			handle_err(err)

			if m.Result == "NOT FOUND, ABORTED" || (m.Command != "ABORT" && m.Result == "ABORTED") { // abort msg, need to notify others
				for k := range serverEncMap {
					err := serverEncMap[k].Encode(ServerMessage{
						Tx_id:    m.Tx_id,
						Coord_id: id,
						Command:  "ABORT"}) // forward the command to the correct branch
					handle_err(err)
				}

				go handle_command(m.Tx_id, "ABORT")
			}
		}

	} else {
		result := handle_command(m.Tx_id, m.Command)

		m.Result = result
		err := serverEncMap[m.Coord_id].Encode(m) // reply with the result
		handle_err(err)
	}
}

func handle_receive_server(conn net.Conn) {
	dec := gob.NewDecoder(conn)

	for {
		var m ServerMessage
		err := dec.Decode(&m)
		if err != nil {
			return // abort decode thread
		}

		go handle_server_msg(m)
	}
}

func handle_client_msg(m ClientMessage) {
	arg := strings.Split(m.Command, " ")

	switch arg[0] {
	case "BEGIN":
		conn, err := net.Dial("tcp", m.Hostname+":"+m.Port)
		handle_err(err)

		enc := gob.NewEncoder(conn)

		err = enc.Encode("OK") // output "OK"
		handle_err(err)

		mu_clientEncMap.Lock()
		clientEncMap[m.Tx_id] = enc
		mu_clientEncMap.Unlock()
	case "COMMIT":
		mu_prepareMap.Lock()
		prepareReplyMap[m.Tx_id] = nil
		mu_prepareMap.Unlock()

		for k := range serverEncMap {
			err := serverEncMap[k].Encode(ServerMessage{
				Tx_id:    m.Tx_id,
				Coord_id: id,
				Command:  "PREPARE"}) // forward the command to the correct branch
			handle_err(err)
		}

		result := handle_command(m.Tx_id, "PREPARE")

		mu_prepareMap.Lock()
		prepareReplyMap[m.Tx_id] = append(prepareReplyMap[m.Tx_id], result)

		if len(prepareReplyMap[m.Tx_id]) < N {
			mu_prepareMap.Unlock()
			return
		}

		if len(prepareReplyMap[m.Tx_id]) == N {
			commitOrAbort := "COMMIT"

			for _, s := range prepareReplyMap[m.Tx_id] {
				if s == "No" {
					commitOrAbort = "ABORT"
					break
				}
			}

			mu_prepareMap.Unlock()

			for k := range serverEncMap {
				err := serverEncMap[k].Encode(ServerMessage{
					Tx_id:    m.Tx_id,
					Coord_id: id,
					Command:  commitOrAbort}) // forward the command to the correct branch
				handle_err(err)
			}

			result := handle_command(m.Tx_id, commitOrAbort)

			mu_clientEncMap.Lock()
			err := clientEncMap[m.Tx_id].Encode(result) // output to the client
			mu_clientEncMap.Unlock()
			handle_err(err)
		}
		return
	case "ABORT":
		for k := range serverEncMap {
			err := serverEncMap[k].Encode(ServerMessage{
				Tx_id:    m.Tx_id,
				Coord_id: id,
				Command:  "ABORT"}) // forward the command to the correct branch
			handle_err(err)
		}

		result := handle_command(m.Tx_id, "ABORT")

		mu_clientEncMap.Lock()
		err := clientEncMap[m.Tx_id].Encode(result) // output to the client
		mu_clientEncMap.Unlock()
		handle_err(err)

		return
	default: // one branch command
		branch := strings.Split(arg[1], ".")[0]
		if branch == id {
			result := handle_command(m.Tx_id, m.Command)

			mu_clientEncMap.Lock()
			err := clientEncMap[m.Tx_id].Encode(result) // output result
			mu_clientEncMap.Unlock()
			handle_err(err)
		} else {
			err := serverEncMap[branch].Encode(ServerMessage{
				Tx_id:    m.Tx_id,
				Coord_id: id,
				Command:  m.Command}) // forward the command to the correct branch
			handle_err(err)
		}
	}
}

func handle_receive_client(conn net.Conn) {
	dec := gob.NewDecoder(conn)

	for {
		var m ClientMessage
		err := dec.Decode(&m)
		if err != nil {
			return // abort decode thread
		}

		go handle_client_msg(m)
	}
}

func main() {
	argv := os.Args[1:]
	if len(argv) != ARG_NUM {
		fmt.Fprintf(os.Stderr, "usage: ./server <branch id> <configuration file>\n")
		os.Exit(1)
	}

	serverEncMap = make(map[string]*gob.Encoder)
	clientEncMap = make(map[int64]*gob.Encoder)
	balanceMap = make(map[string]*balance)
	prepareReplyMap = make(map[int64][]string)

	id = argv[0]
	config := argv[1]

	// read config file
	file, err := os.Open(config)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)

	node_name := make([]string, N)
	serv_addr := make([]string, N)
	serv_port := make([]string, N)

	for i := 0; i < N; i++ {
		scanner.Scan()
		node_name[i] = scanner.Text()
		scanner.Scan()
		serv_addr[i] = scanner.Text()
		scanner.Scan()
		serv_port[i] = scanner.Text()
	}

	var ln net.Listener

	for i, v := range node_name {
		if v == id {
			ln, err = net.Listen("tcp", serv_addr[i]+":"+serv_port[i])
			handle_err(err)
		} else {
			conn, err := net.Dial("tcp", serv_addr[i]+":"+serv_port[i])
			for err != nil {
				time.Sleep(1 * time.Second)
				conn, err = net.Dial("tcp", serv_addr[i]+":"+serv_port[i])
			}
			enc := gob.NewEncoder(conn)
			serverEncMap[v] = enc
		}
	}

	defer ln.Close()

	for i := 0; i < N-1; i++ {
		conn, err := ln.Accept()
		handle_err(err)
		go handle_receive_server(conn)
	}

	for {
		conn, err := ln.Accept()
		handle_err(err)
		go handle_receive_client(conn)
	}
}
