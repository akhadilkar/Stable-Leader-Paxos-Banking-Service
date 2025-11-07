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

func statusChar(st SlotStatus) string {
	switch st {
	case StatusEmpty:
		return "X"
	case StatusAccepted:
		return "A"
	case StatusCommitted:
		return "C"
	case StatusExecuted:
		return "E"
	default:
		return "?"
	}
}

func repl(n *Node) {
	n.mu.Lock()
	if n.lastLeaderMsg.IsZero() {
		n.lastLeaderMsg = time.Now()
	}
	n.mu.Unlock()

	in := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !in.Scan() {
			return
		}

		cmdLine := strings.TrimSpace(in.Text())
		if cmdLine == "" {
			fmt.Println("commands: live, bank, tx, leader, setleader, help")
			continue
		}
		fields := strings.Fields(cmdLine)
		cmd := strings.ToLower(fields[0])

		switch cmd {
		case "live":
			live := n.discoverLivePeers(250 * time.Millisecond)
			if len(live) == 0 {
				fmt.Println("live: (none exept self)")
			} else {
				fmt.Printf("live: %s\n", peersString(live))
			}

		case "printdb":
			snap := n.printDB()
			keys := make([]string, 0, len(snap))
			for k := range snap {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Printf("  %s: %d\n", k, snap[k])
			}

		case "leader":
			fmt.Printf("isLeader = %v\n", n.isLeader)

		case "setleader":
			if len(fields) != 2 {
				fmt.Println("usage: setleader true|false")
				break
			}
			v := strings.ToLower(fields[1])
			n.mu.Lock()
			n.isLeader = (v == "true" || v == "1" || v == "yes")
			n.LeaderBallot = Ballot{Round: 1, NodeID: n.id}
			n.HighestBallotSeen = n.LeaderBallot
			n.mu.Unlock()
			fmt.Printf("isLeader = %v\n", n.isLeader)
			fmt.Printf("became leader with ballot (%d,%d)\n", n.LeaderBallot.Round, n.LeaderBallot.NodeID)

		case "rsubmit":

			if len(fields) != 5 {
				fmt.Println("usage: rsubmit <peerID> <from> <to> <amt>")
				break
			}
			pid, err := strconv.Atoi(fields[1])
			if err != nil || pid < 1 {
				fmt.Println("peerID must be a positive integer")
				break
			}
			addr, ok := addrOfPeer(n, pid)
			if !ok {
				fmt.Println("unknown peer id")
				break
			}
			amt, err := strconv.ParseInt(fields[4], 10, 64)
			if err != nil || amt <= 0 {
				fmt.Println("amount must be positive integer")
				break
			}
			args := &SubmitArgs{From: fields[2], To: fields[3], Amt: amt}
			var rep SubmitReply
			if err := rpcCall(addr, "Node.Submit", args, &rep); err != nil {
				fmt.Printf("rsubmit -> %d@%s: %v\n", pid, addr, err)
				break
			}
			fmt.Printf("rsubmit -> %d@%s: %v (%s)\n", pid, addr, rep.OK, rep.Msg)

		case "rbank":

			if len(fields) != 2 {
				fmt.Println("usage: rbank <peerID>")
				break
			}
			pid, err := strconv.Atoi(fields[1])
			if err != nil || pid < 1 {
				fmt.Println("peerID must be a positive integer")
				break
			}
			addr, ok := addrOfPeer(n, pid)
			if !ok {
				fmt.Println("unknown peer id")
				break
			}
			var out BankReply
			if err := rpcCall(addr, "Node.GetBank", &Empty{}, &out); err != nil {
				fmt.Printf("rbank %d: %v\n", pid, err)
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

		case "who":

			type claim struct {
				id   int
				addr string
			}
			var leaders []claim
			for _, p := range n.peers {
				var s StatusReply
				if err := rpcCall(p.Addr, "Node.GetStatus", &Empty{}, &s); err == nil && s.IsLeader {
					leaders = append(leaders, claim{id: s.ID, addr: s.Addr})
				}
			}
			if len(leaders) == 0 {
				fmt.Println("who: no leader found")
			} else {
				for _, c := range leaders {
					fmt.Printf("leader: %d@%s\n", c.id, c.addr)
				}
				if len(leaders) > 1 {
					fmt.Println("(warning: multiple leaders claimed)")
				}
			}

		case "submit":
			if len(fields) != 4 {
				fmt.Println("usage: submit <from> <to> <amt>")
				break
			}
			amt, err := strconv.ParseInt(fields[3], 10, 64)
			if err != nil || amt <= 0 {
				fmt.Println("amount must be positive integer")
				break
			}

			tryAddr := n.addr
			for hop := 0; hop < 3; hop++ {
				var rep SubmitReply
				c := ClientID("REPL")
				args := SubmitArgs{From: fields[1], To: fields[2], Amt: amt, C: c, Tau: nextTau(c)}
				err := rpcCall(tryAddr, "Node.Submit", &args, &rep)
				if err != nil {
					fmt.Printf("submit -> %s: %v\n", tryAddr, err)
					break
				}
				if rep.OK {
					fmt.Printf("submit -> %s: %s\n", tryAddr, rep.Msg)
					break
				}
				if rep.Redirect == "" {
					fmt.Printf("submit -> %s: %s (no redirect)\n", tryAddr, rep.Msg)
					break
				}
				tryAddr = rep.Redirect
			}

		case "status":
			n.mu.Lock()
			fmt.Printf(
				"id=%d addr=%s leader=%v leaderBallot=(%d,%d) highestBallot=(%d,%d) nextSeq=%d commitIndex=%d executeIndex=%d\n",
				n.id, n.addr, n.isLeader,
				n.LeaderBallot.Round, n.LeaderBallot.NodeID,
				n.HighestBallotSeen.Round, n.HighestBallotSeen.NodeID,
				n.NextSeq, n.CommitIndex, n.ExecuteIndex,
			)
			n.mu.Unlock()
		case "log":
			n.mu.Lock()
			ks := make([]int, 0, len(n.AcceptLog))
			for s := range n.AcceptLog {
				ks = append(ks, s)
			}
			sort.Ints(ks)
			if len(ks) == 0 {
				fmt.Println("(log empty)")
			} else {
				for _, s := range ks {
					e := n.AcceptLog[s]
					st := n.SlotStatus[s]
					fmt.Printf("slot=%d st=%s tx=%s->%s %d ballot=(%d,%d)\n",
						s, statusChar(st),
						e.AcceptVal.T.From, e.AcceptVal.T.To, e.AcceptVal.T.Amt,
						e.AcceptNum.Round, e.AcceptNum.NodeID)
				}
			}
			n.mu.Unlock()
		case "fail":
			n.mu.Lock()
			n.failed = true
			n.isLeader = false
			n.mu.Unlock()
			fmt.Println("node is now FAILED: dropping all RPCs")
		case "recover", "restart":
			n.mu.Lock()
			n.failed = false
			n.mu.Unlock()
			fmt.Println("node RECOVERED: serving RPCs again; catching up…")
			//go n.catchUp()
			n.catchUp()

		case "help":
			fmt.Println("commands: live, printdb, tx, leader, setleader, help, submit, rsubmit, rbank, who, submit  (Ctrl+C to quit)")
		case "quit", "exit":
			os.Exit(0)
		default:
			fmt.Println("unknown; try 'help'")
		}
	}
}
