package main

import (
	"bufio"
	"container/heap"
	"encoding/gob"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
	"strings"
	"sort"
)

type ID struct {
	Node_id string
    Num int
}

type TX struct {
	Tx_id ID
	Priority float64
    Text string
	Deliverable bool
	// Discarded bool
	// Need_p_count int
}

type Message struct {
	Message_id ID
    Tx TX
}

type PriorityQueue []*TX

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x any) {
	*pq = append(*pq, x.(*TX))
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	x := old[n-1]
	*pq = old[0 : n-1]
	return x
}

const ARG_NUM int = 2

var n int // num of nodes in the cluster
var id string // current node
var group []*gob.Encoder

var mu_seq_num sync.Mutex
var seq_num int // num of messages current node has sent
var mu_received sync.Mutex
var received map[ID]TX // msg received, m.ID: m.TX
var delivered map[ID]bool // tx delivered, tx.ID: bool 
var mu_pq sync.Mutex
var pq PriorityQueue
var max float64 // current max priority

var p_map map[int][]float64 // priority map
var p_map_id map[int][]string

var balance_map map[string]int


var mu_failure sync.Mutex


var wg sync.WaitGroup
var err error

var f_logger *os.File

func logger(tx TX) {
	timeinmicros := time.Now().UnixNano() / 1000
	timeinsecs := float64(timeinmicros) / 1000000
	fmt.Fprintf(f_logger, "%.6f ", timeinsecs)
	fmt.Fprintln(f_logger, tx)
}

func print_balance() {
	accs := make([]string, 0, len(balance_map))
	for a := range balance_map {
		accs = append(accs, a)
	}
	sort.Strings(accs)

	fmt.Print("BALANCES ")
	for _, a := range accs {
		if balance_map[a] <= 0 {
			continue
		}
		fmt.Print(a, ":", balance_map[a], " ")
	}
	fmt.Print("\n")
}

func deposit(acc string, amount int) {
	if _, found := balance_map[acc]; !found {
		balance_map[acc] = 0
	}
	balance_map[acc] = balance_map[acc] + amount
	print_balance()
}

func transfer(source_acc, recv_acc string, amount int) {
	if _, found := balance_map[source_acc]; found {
		source_balance := balance_map[source_acc]
		if source_balance >= amount {
			balance_map[source_acc] = source_balance - amount
			deposit(recv_acc, amount)
		} else {
			print_balance()
		}
	}
}

func balance(tx TX) {
	go logger(tx)

	tx_text := tx.Text

	// fmt.Println("************")
	// fmt.Println(tx_text)
	// fmt.Println("************")

	var amount int
	sep := strings.Split(tx_text, " ")

	if sep[0] == "DEPOSIT" {
		amount, err = strconv.Atoi(sep[2])
		deposit(sep[1], amount)
	} else {
		amount, err = strconv.Atoi(sep[4])
		transfer(sep[1], sep[3], amount)
	}
}

func max_priority(idx int) float64 {
	max_p := p_map[idx][0]
	for _, p := range p_map[idx] {
		if max_p < p {
			max_p = p
		}
	}
	return max_p
}

func handle_dead(dead_idx int) {
	// If a node fails, discard transaction whose final priority is unable to be decided or the initiating node is dead
	dead_node_id := group_node_ids[dead_idx]

	mu_pq.Lock()

	for i := 0; pq.Len() > 0 && i < pq.Len(); i++ {
		if pq[i].Deliverable == false { // delete tx that final priority will never arrived
			if pq[i].Tx_id.Node_id == dead_node_id {
				heap.Remove(&pq, i)
				i--
			} else if pq[i].Tx_id.Node_id == id { // self initiated transaction
				for j, v := range p_map_id[pq[i].Tx_id.Num] {
					if v == dead_node_id {
						// delete a[j]
						a := p_map[pq[i].Tx_id.Num]

						a[j] = a[len(a)-1]
						p_map[pq[i].Tx_id.Num] = a[:len(a)-1]

						// delete b[j]
						b := p_map_id[pq[i].Tx_id.Num]

						b[j] = b[len(b)-1]
						p_map_id[pq[i].Tx_id.Num] = b[:len(b)-1]

						break
					}
				}
			}
		}
	}
	
	mu_pq.Unlock()
}

func handle_err(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func handle_transaction(id string) {
	tx_num := 0 // num of transactions current node has sent

	s, err := strconv.ParseFloat(id[len(id)-1:], 64)
	handle_err(err)

	reader := bufio.NewReader(os.Stdin)
	for {
		text, _ := reader.ReadString('\n')

		priority := s*0.1

		mu_pq.Lock()

		integer, _ := math.Modf(max)
		priority += integer + 1

		

		tx := TX{ID{id, tx_num}, priority, text[:len(text)-1], false}

		go logger(tx)
		
		heap.Push(&pq, &tx)
		p_map[tx_num] = append(p_map[tx_num], priority)
		p_map_id[tx_num] = append(p_map_id[tx_num], id)
		if max < priority {
			max = priority
		}

		mu_pq.Unlock()

		rMulticast(tx)

		tx_num++
	}
}

var failure []bool
var group_node_ids []string

func handle_receive(conn net.Conn) {
	dec := gob.NewDecoder(conn)

	for {
		var m Message
		err := dec.Decode(&m)
		if err != nil {
			return // abort decode thread
		}
	
		bDeliver(m)
	}
}

func rMulticast(tx TX) {
	mu_seq_num.Lock()
	multicast_id := seq_num
	seq_num++
	mu_seq_num.Unlock()

	bMulticast(Message{ID{id, multicast_id}, tx})
}

func bMulticast(m Message) {
	for i, enc := range group {
		mu_failure.Lock()
		if failure[i] == false {
			err := enc.Encode(m)
			if err != nil {
				failure[i] = true
				defer handle_dead(i)
				n--
			}
		}
		mu_failure.Unlock()
	}

	bDeliver(m)
}

func bDeliver(m Message) {
	mu_received.Lock()

	_, ok := received[m.Message_id]

	// R-multicast doesn't receive duplicated messages
	if ok == false {
		received[m.Message_id] = m.Tx
		defer rDeliver(m)
		if m.Message_id.Node_id != id {
			defer bMulticast(m)
		}
	}
	
	mu_received.Unlock()
}

func rDeliver(m Message) {
	s, err := strconv.ParseFloat(id[len(id)-1:], 64)
	handle_err(err)

	tx := m.Tx

	mu_pq.Lock()
	// fmt.Println(m)
	for i := range pq {
		if pq[i].Tx_id == m.Tx.Tx_id {			
			if pq[i].Tx_id.Node_id == id && m.Message_id.Node_id != id { // self initiated transaction => waiting for proposed priority
				// fmt.Println("get proposed")
				p_map[pq[i].Tx_id.Num] = append(p_map[pq[i].Tx_id.Num], m.Tx.Priority)
				p_map_id[pq[i].Tx_id.Num] = append(p_map_id[pq[i].Tx_id.Num], m.Message_id.Node_id)

				if len(p_map[pq[i].Tx_id.Num]) >= n { // when get all proposed priorities
					// fmt.Println("get all proposed")
					pqi := TX{pq[i].Tx_id, max_priority(pq[i].Tx_id.Num), pq[i].Text, true}

					pq[i] = &pqi
					heap.Fix(&pq, i)
					
					defer rMulticast(pqi)

					for pq.Len() > 0 && pq[0].Deliverable == true {
						tmp := *heap.Pop(&pq).(*TX)
						delivered[tmp.Tx_id] = true
						balance(tmp)
					}
					
				}

				mu_pq.Unlock()
				return
			} else if pq[i].Tx_id.Node_id != id && pq[i].Tx_id.Node_id == m.Message_id.Node_id { // not initiated, in queue => get final priority
				// fmt.Println("get final")
				pq[i] = &TX{pq[i].Tx_id, m.Tx.Priority, pq[i].Text, true}
				
				heap.Fix(&pq, i)

				for pq.Len() > 0 && pq[0].Deliverable == true {
					tmp := *heap.Pop(&pq).(*TX)
					delivered[tmp.Tx_id] = true
					balance(tmp)
				}

				mu_pq.Unlock()
				return
			} else { // first multicast, self-deliver just after generating a tx
				mu_pq.Unlock()
				return
			}
		}
	}

	// don't get delivered transaction
	if _, found := delivered[m.Tx.Tx_id]; found {
		// fmt.Println("delivered already")
		mu_pq.Unlock()
		return
	}

	// don't get discarded transaction
	if m.Tx.Tx_id.Node_id == id {
		// fmt.Println("dead already")
		mu_pq.Unlock()
		return
	}

	// not intiated, not in queue => send proposed priority
	if m.Tx.Tx_id.Node_id == m.Message_id.Node_id {
		integer, _ := math.Modf(max)
		tx.Priority = integer + 1 + s*0.1

		heap.Push(&pq, &tx)
		if max < tx.Priority {
			max = tx.Priority
		}

		mu_pq.Unlock()

		defer rMulticast(tx)
	}
}

func main()  {
	argv := os.Args[1:]
	if (len(argv) != ARG_NUM) {
		fmt.Fprintf(os.Stderr, "usage: ./mp1_node <identifier> <configuration file>\n")
		os.Exit(1)
	}

	received = make(map[ID]TX)
	delivered = make(map[ID]bool)
	p_map = make(map[int][]float64)
	p_map_id = make(map[int][]string)
	balance_map = make(map[string]int)
	heap.Init(&pq)

	id = argv[0]
	config := argv[1]

	f_logger, err = os.Create("log_"+id+".txt")
	if err != nil {
		log.Fatal(err)
	}
	defer f_logger.Close()

	// read config file
	file, err := os.Open(config)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)
	
	scanner.Scan()
	n, err = strconv.Atoi(scanner.Text())
	handle_err(err)

	failure = make([]bool, n-1)

	node_name := make([]string, n)
	serv_addr := make([]string, n)
	serv_port := make([]string, n)

	for i := 0; i < n; i++ {
		scanner.Scan()
		node_name[i] = scanner.Text()
		scanner.Scan()
		serv_addr[i] = scanner.Text()
		scanner.Scan()
		serv_port[i] = scanner.Text()
	}

	var ln net.Listener

	// tcp Dial
	for i, v := range node_name {
		if v == id {
			ln, err = net.Listen("tcp", serv_addr[i] + ":" + serv_port[i])
			handle_err(err)
		} else {
			conn, err := net.Dial("tcp", serv_addr[i] + ":" + serv_port[i])
			for err != nil {
				time.Sleep(1*time.Second)
				conn, err = net.Dial("tcp", serv_addr[i] + ":" + serv_port[i])
			}
			
			enc := gob.NewEncoder(conn)
			group = append(group, enc)
			group_node_ids = append(group_node_ids, v)
			
		}
	}

	defer ln.Close()

	// tcp Accept
	for i := 0; i < n-1; i++ {
		conn, err := ln.Accept()
		handle_err(err)
		go handle_receive(conn)
	}

	wg.Add(1)
	go handle_transaction(id)
	wg.Wait()
}
