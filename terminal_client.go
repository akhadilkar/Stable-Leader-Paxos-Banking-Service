package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var tauByClient = map[ClientID]int64{}

func nextTau(c ClientID) int64 {
	t := tauByClient[c]
	t++
	tauByClient[c] = t
	return t
}

func submitToLeader(peers []Peer, args SubmitArgs) {

	addr := discoverLeaderAddrFromPeers(peers)
	if addr == "" {
		addr = peers[0].Addr
	}

	deadline := time.Now().Add(3 * time.Second)
	backoff := 80 * time.Millisecond

	peerIndex := func(a string) int {
		for i := range peers {
			if peers[i].Addr == a {
				return i
			}
		}
		return 0
	}

	for {
		var rep SubmitReply
		err := rpcCall(addr, "Node.Submit", &args, &rep)

		if err == nil && rep.OK {
			fmt.Printf("submit -> %s: %s\n", addr, rep.Msg)
			return
		}

		if err == nil && !rep.OK {

			if rep.Redirect != "" && rep.Redirect != addr {
				addr = rep.Redirect
				continue
			}

			if rep.Msg == "redirect" && rep.Redirect == "" {
				if time.Now().After(deadline) {
					fmt.Printf("submit -> %s: giving up (no leader elected yet)\n", addr)
					return
				}
				time.Sleep(backoff)
				if newAddr := discoverLeaderAddrFromPeers(peers); newAddr != "" {
					addr = newAddr
				}

				if backoff < 400*time.Millisecond {
					backoff += 40 * time.Millisecond
				}
				continue
			}

			fmt.Printf("submit -> %s: %s\n", addr, rep.Msg)
			return
		}

		if time.Now().After(deadline) {
			fmt.Printf("submit -> %s: giving up (network)\n", addr)
			return
		}
		i := peerIndex(addr)
		addr = peers[(i+1)%len(peers)].Addr
	}
}

func runClientsRepl(base, n int) {

	peers := make([]Peer, n)
	for i := 0; i < n; i++ {
		peers[i] = Peer{ID: i + 1, Addr: fmt.Sprintf(":%d", base+i)}
	}

	help := func() {
		fmt.Println("client commands:")
		fmt.Println("  help")
		fmt.Println("  who                     # discover leader")
		fmt.Println("  live                    # which nodes respond")
		fmt.Println("  submit <from> <to> <amt># send tx to leader (auto-redirect)")
		fmt.Println("  bank [peerID]           # balances from peer (default: leader)")
		fmt.Println("  quit/exit")
	}

	discoverLeader := func() string {
		for _, p := range peers {
			var s StatusReply
			if err := rpcCall(p.Addr, "Node.GetStatus", &Empty{}, &s); err == nil && s.IsLeader {
				return s.Addr
			}
		}
		return ""
	}

	liveList := func() []Peer {
		var up []Peer
		for _, p := range peers {
			var out Pong
			if err := rpcCall(p.Addr, "Node.Ping", &Empty{}, &out); err == nil {
				up = append(up, p)
			}
		}
		return up
	}

	help()
	in := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("client> ")
		if !in.Scan() {
			return
		}
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		fs := strings.Fields(line)
		switch strings.ToLower(fs[0]) {
		case "help":
			help()
		case "quit", "exit":
			return
		case "who":
			if l := discoverLeader(); l != "" {
				fmt.Println("leader:", l)
			} else {
				fmt.Println("leader: (none)")
			}
		case "live":
			up := liveList()
			if len(up) == 0 {
				fmt.Println("live: (none)")
				break
			}
			parts := make([]string, 0, len(up))
			for _, p := range up {
				parts = append(parts, fmt.Sprintf("%d@%s", p.ID, p.Addr))
			}
			fmt.Println("live:", strings.Join(parts, ", "))
		case "submit":
			if len(fs) != 4 {
				fmt.Println("usage: submit <from> <to> <amt>")
				break
			}
			amt, err := strconv.ParseInt(fs[3], 10, 64)
			if err != nil || amt <= 0 {
				fmt.Println("amount must be positive integer")
				break
			}
			c := ClientID("CLI")
			args := SubmitArgs{From: fs[1], To: fs[2], Amt: amt, C: c, Tau: nextTau(c)}
			submitToLeader(peers, args)

		case "bank":
			var addr string
			if len(fs) == 2 {
				id, err := strconv.Atoi(fs[1])
				if err != nil || id < 1 || id > len(peers) {
					fmt.Println("bank <peerID>")
					break
				}
				addr = peers[id-1].Addr
			} else {
				addr = discoverLeader()
				if addr == "" {
					addr = peers[0].Addr
				}
			}
			var out BankReply
			if err := rpcCall(addr, "Node.GetBank", &Empty{}, &out); err != nil {
				fmt.Printf("bank %s: %v\n", addr, err)
				break
			}
			keys := make([]string, 0, len(out.Accounts))
			for k := range out.Accounts {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Printf("  %s: %d\n", k, out.Accounts[k])
			}
		default:
			fmt.Println("unknown; try 'help'")
		}
	}
}
