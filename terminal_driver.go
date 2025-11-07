package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var tauByClientDriver = map[ClientID]int64{}

func nextTauDriver(c ClientID) int64 { tauByClientDriver[c]++; return tauByClientDriver[c] }

func waitForAllNodes(peers []Peer) {
	for {
		missing := []int{}
		for _, p := range peers {
			var s StatusReply
			if err := rpcCall(p.Addr, "Node.GetStatus", &Empty{}, &s); err != nil {
				missing = append(missing, p.ID)
			}
		}
		if len(missing) == 0 {
			return
		}
		fmt.Printf("Waiting for nodes to start: %v\n", missing)
		time.Sleep(800 * time.Millisecond)
	}
}

func printStatusAll(peers []Peer) {
	for _, p := range peers {
		var s StatusReply
		if err := rpcCall(p.Addr, "Node.GetStatus", &Empty{}, &s); err != nil {
			fmt.Printf("n%d %s (DOWN: %v)\n", p.ID, p.Addr, err)
			continue
		}

		fmt.Printf("n%d %s leader=%v\n", s.ID, s.Addr, s.IsLeader)
	}
}

func printBank(peers []Peer, id int) {
	if id == 0 {
		for _, p := range peers {
			printBank(peers, p.ID)
		}
		return
	}
	addr := peers[id-1].Addr
	var b BankReply
	if err := rpcCall(addr, "Node.GetBank", &Empty{}, &b); err != nil {
		fmt.Printf("n%d bank: %v\n", id, err)
		return
	}
	fmt.Printf("n%d bank:\n", id)
	keys := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	for _, k := range keys {
		fmt.Printf("  %s: %d\n", k, b.Accounts[k])
	}
}

func printLog(peers []Peer, id int) {
	if id == 0 {
		for _, p := range peers {
			printLog(peers, p.ID)
		}
		return
	}
	addr := peers[id-1].Addr
	var lr LogReply
	if err := rpcCall(addr, "Node.GetLog", &Empty{}, &lr); err != nil {
		fmt.Printf("n%d log: %v\n", id, err)
		return
	}
	fmt.Printf("n%d log:\n", id)
	if len(lr.Entries) == 0 {
		fmt.Println("  (empty)")
		return
	}
	for _, e := range lr.Entries {
		fmt.Printf("  slot=%d st=%s tx=%s->%s %d ballot=(%d,%d)\n",
			e.Seq, e.Status, e.From, e.To, e.Amt, e.Ballot.Round, e.Ballot.NodeID)
	}
}

func discoverLeader(peers []Peer) int {
	for _, p := range peers {
		var s StatusReply
		if err := rpcCall(p.Addr, "Node.GetStatus", &Empty{}, &s); err == nil && s.IsLeader {
			return s.ID
		}
	}
	return 0
}

func waitForLeader(peers []Peer, live map[int]bool, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
		id := discoverLeader(peers)
		if id != 0 && live[id] {
			return id
		}
		if time.Now().After(deadline) {
			return 0
		}
		time.Sleep(80 * time.Millisecond)
	}
}

func runDriver(csvPath string, basePort, total int) {

	peers := make([]Peer, total)
	for i := 0; i < total; i++ {
		peers[i] = Peer{ID: i + 1, Addr: fmt.Sprintf(":%d", basePort+i)}
	}

	fmt.Printf("Driver waiting for %d nodes to be UP...\n", total)
	waitForAllNodes(peers)
	fmt.Println("All nodes are UP.")
	printStatusAll(peers)

	f, err := os.Open(csvPath)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		panic(err)
	}
	if len(rows) == 0 {
		fmt.Println("empty CSV")
		return
	}

	reLive := regexp.MustCompile(`\[(.*?)\]`)

	type Tx struct {
		From, To string
		Amt      int64
		LF       bool
	}
	type Set struct {
		Live []int
		Txs  []Tx
	}
	sets := map[int]*Set{}
	order := []int{}
	cur := 0

	for i, row := range rows {
		if i == 0 {
			continue
		}
		if len(row) < 3 {
			continue
		}
		setS := strings.TrimSpace(row[0])
		txS := strings.TrimSpace(row[1])
		liveS := strings.TrimSpace(row[2])

		if setS != "" {
			v, _ := strconv.Atoi(setS)
			cur = v
			if sets[cur] == nil {
				sets[cur] = &Set{}
				order = append(order, cur)
			}
		}
		s := sets[cur]
		if s == nil {
			continue
		}
		if liveS != "" {

			if m := reLive.FindStringSubmatch(liveS); m != nil {
				liveS = m[1]
			}

			reID := regexp.MustCompile(`n?\s*(\d+)`)
			ms := reID.FindAllStringSubmatch(liveS, -1)
			seen := map[int]bool{}
			var live []int
			for _, m := range ms {
				id, err := strconv.Atoi(m[1])
				if err != nil {
					continue
				}
				if !seen[id] {
					live = append(live, id)
					seen[id] = true
				}
			}
			sort.Ints(live)
			s.Live = live
		}

		reAny := regexp.MustCompile(`LF|\(\s*([A-J])\s*,\s*([A-J])\s*,\s*(\d+)\s*\)`)
		for _, m := range reAny.FindAllStringSubmatch(txS, -1) {
			if strings.HasPrefix(m[0], "LF") {
				s.Txs = append(s.Txs, Tx{LF: true})
				continue
			}

			amt, _ := strconv.ParseInt(m[3], 10, 64)
			s.Txs = append(s.Txs, Tx{From: m[1], To: m[2], Amt: amt})
		}
	}

	in := bufio.NewScanner(os.Stdin)
	for _, sn := range order {
		s := sets[sn]
		fmt.Printf("\n=== SET %d ===\n", sn)
		fmt.Printf("Parsed %d operations: ", len(s.Txs))
		if len(s.Txs) == 0 {
			fmt.Println("(none)")
		} else {
			items := make([]string, 0, len(s.Txs))
			for _, tx := range s.Txs {
				if tx.LF {
					items = append(items, "LF")
				} else {
					items = append(items, fmt.Sprintf("(%s,%s,%d)", tx.From, tx.To, tx.Amt))
				}
			}
			fmt.Println(strings.Join(items, " "))
		}

		liveMap := map[int]bool{}
		for _, id := range s.Live {
			liveMap[id] = true
		}
		for _, p := range peers {
			_ = rpcCall(p.Addr, "Node.SetFailed", &FailArgs{Failed: !liveMap[p.ID]}, &Empty{})
		}

		leaderID := waitForLeader(peers, liveMap, 1500*time.Millisecond)

		fmt.Printf("Live: %v  Leader: ", s.Live)
		if leaderID == 0 {
			fmt.Println("(none yet)")
		} else {
			fmt.Printf("n%d\n", leaderID)
		}
		printStatusAll(peers)

		if leaderID == 0 {
			leaderID = discoverLeader(peers)
		}
		if leaderID != 0 {
			var rr RetryReply
			_ = rpcCall(peers[leaderID-1].Addr, "Node.RetryPending", &Empty{}, &rr)
			if rr.Count > 0 {
				fmt.Printf("Retried %d pending slots on leader n%d\n", rr.Count, leaderID)
			}
		}

		major := (total / 2) + 1
		liveCount := 0
		for _, ok := range liveMap {
			if ok {
				liveCount++
			}
		}
		if liveCount < major {
			fmt.Printf("WARNING: live=%d < majority=%d (N=%d). Commits will fail in this set (by design).\n",
				liveCount, major, total)
		}

		for i, tx := range s.Txs {
			if tx.LF {

				if leaderID == 0 {
					leaderID = discoverLeader(peers)
				}
				if leaderID != 0 {
					_ = rpcCall(peers[leaderID-1].Addr, "Node.SetFailed", &FailArgs{Failed: true}, &Empty{})
					fmt.Printf(" op %d: LF -> failed leader n%d\n", i+1, leaderID)
					liveMap[leaderID] = false
				}

				leaderID = waitForLeader(peers, liveMap, 1200*time.Millisecond)
				if leaderID == 0 {
					fmt.Println("          new leader: (none yet)")
				} else {
					fmt.Printf("          new leader n%d\n", leaderID)
				}
				continue
			}

			c := ClientID("CSV")
			args := SubmitArgs{From: tx.From, To: tx.To, Amt: tx.Amt, C: c, Tau: nextTauDriver(c)}
			submitToLeader(peers, args)
		}

		fmt.Println("\nPause: type one of:  status | db [id|all] | log [id|all] | who | continue | quit/exit")
		for {
			fmt.Print("driver> ")
			if !in.Scan() {
				return
			}
			cmd := strings.Fields(strings.TrimSpace(in.Text()))
			if len(cmd) == 0 || cmd[0] == "continue" || cmd[0] == "next" || cmd[0] == "proceed" {
				break
			}
			if cmd[0] == "quit" || cmd[0] == "exit" {
				fmt.Println("bye")
				os.Exit(0)
			}
			switch cmd[0] {
			case "status":
				printStatusAll(peers)
			case "who":
				ldr := discoverLeader(peers)
				if ldr == 0 {
					fmt.Println("leader: (none)")
				} else {
					fmt.Printf("leader: n%d\n", ldr)
				}
			case "db":
				if len(cmd) == 1 || cmd[1] == "all" {
					printBank(peers, 0)
				} else {
					id, _ := strconv.Atoi(cmd[1])
					printBank(peers, id)
				}
			case "log":
				if len(cmd) == 1 || cmd[1] == "all" {
					printLog(peers, 0)
				} else {
					id, _ := strconv.Atoi(cmd[1])
					printLog(peers, id)
				}
			default:
				fmt.Println("(unknown; try: status | db [id|all] | log [id|all] | who | continue)")
			}
		}
	}
	fmt.Println("\nAll sets done.")
}
