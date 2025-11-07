package main

import (
	"fmt"
	"net"
	"net/rpc"
	"strings"
	"time"
)

const (
	tElection        = 700 * time.Millisecond
	tPrepareCooldown = 400 * time.Millisecond
)

func (n *Node) discoverLivePeers(timeout time.Duration) []Peer {
	var live []Peer
	for _, p := range n.peers {
		if p.ID == n.id {
			continue // skip self
		}
		conn, err := net.DialTimeout("tcp", p.Addr, timeout)
		if err != nil {
			continue
		}
		cli := rpc.NewClient(conn)
		var out Pong
		err = cli.Call("Node.Ping", &Empty{}, &out)
		_ = cli.Close()
		if err == nil {
			live = append(live, p)
		}
	}
	return live
}

func peersString(ps []Peer) string {
	parts := make([]string, 0, len(ps))
	for _, p := range ps {
		parts = append(parts, fmt.Sprintf("%d@%s", p.ID, p.Addr))
	}
	return strings.Join(parts, ", ")
}

func (n *Node) initAccounts() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.bank == nil {
		n.bank = make(map[string]int64, 10)
	}
	for ch := 'A'; ch <= 'J'; ch++ {
		name := string(ch)
		if _, ok := n.bank[name]; !ok {
			n.bank[name] = 10
		}
	}
}

func (n *Node) printDB() map[string]int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[string]int64, len(n.bank))
	for k, v := range n.bank {
		out[k] = v
	}
	return out
}

func (b Ballot) Less(o Ballot) bool {
	if b.Round != o.Round {
		return b.Round < o.Round
	}
	return b.NodeID < o.NodeID
}

func (n *Node) catchUp() {
	for tries := 0; tries < 8; tries++ {
		addr := discoverLeaderAddr(n)
		if addr == "" {
			time.Sleep(150 * time.Millisecond)
			continue
		}

		n.mu.Lock()
		from := n.ExecuteIndex + 1
		n.mu.Unlock()

		var out FetchReply
		if err := rpcCall(addr, "Node.GetCommitted", &FetchArgs{From: from}, &out); err != nil {
			time.Sleep(150 * time.Millisecond)
			continue
		}

		n.mu.Lock()

		for _, e := range out.Entries {
			n.AcceptLog[e.AcceptSeq] = e
			if n.SlotStatus[e.AcceptSeq] < StatusCommitted {
				n.SlotStatus[e.AcceptSeq] = StatusCommitted
			}
			if e.AcceptSeq > n.CommitIndex {
				n.CommitIndex = e.AcceptSeq
			}
		}

		for n.ExecuteIndex+1 <= n.CommitIndex {
			seq := n.ExecuteIndex + 1
			e, ok := n.AcceptLog[seq]
			if !ok {
				break
			}
			cid := e.AcceptVal.C
			tau := e.AcceptVal.Tau
			prev, seen := n.LastTau[cid]
			if !seen || tau > prev {
				from, to, amt := e.AcceptVal.T.From, e.AcceptVal.T.To, e.AcceptVal.T.Amt
				if n.bank[from] >= amt {
					n.bank[from] -= amt
					n.bank[to] += amt
					n.LastReply[cid] = ReplyArgs{OK: true, Msg: fmt.Sprintf("committed at slot %d", seq)}
				} else {
					n.LastReply[cid] = ReplyArgs{OK: false, Msg: fmt.Sprintf("failed at slot %d: insufficient funds", seq)}
				}
				n.LastTau[cid] = tau
			}

			n.ExecuteIndex = seq
			n.SlotStatus[seq] = StatusExecuted

		}
		n.mu.Unlock()
		return
	}
}

// func (n *Node) noteLeaderMessage(ballot Ballot, leaderID int, leaderAddr string) {
// 	n.mu.Lock()
// 	if ballot.Round > n.HighestBallotSeen.Round ||
// 		(ballot.Round == n.HighestBallotSeen.Round && ballot.NodeID > n.HighestBallotSeen.NodeID) {
// 		n.HighestBallotSeen = ballot
// 	}
// 	if leaderID != 0 {
// 		n.LeaderID = leaderID
// 		n.LeaderAddr = leaderAddr
// 	}
// 	n.lastLeaderMsg = time.Now()
// 	n.mu.Unlock()
// }

func (n *Node) electionLoop() {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()

	for range tick.C {
		n.mu.Lock()
		if n.failed || n.isLeader {
			n.mu.Unlock()
			continue
		}
		silent := time.Since(n.lastLeaderMsg)
		recent := time.Since(n.lastPrepareSeen)
		hb := n.HighestBallotSeen
		myID, myAddr := n.id, n.addr
		peers := append([]Peer(nil), n.peers...)
		n.mu.Unlock()

		if silent < tElection || recent < tPrepareCooldown {
			continue
		}

		cand := Ballot{Round: hb.Round + 1, NodeID: myID}
		args := PrepareArgs{Ballot: cand, CandidateID: myID, CandidateAddr: myAddr}

		ok := 1
		merged := map[int]AcceptLogEntry{}
		maxCommit := 0

		for _, p := range peers {
			if p.ID == myID {
				continue
			}
			var rep PrepareReply
			if err := rpcCall(p.Addr, "Node.Prepare", &args, &rep); err != nil {
				continue
			}
			if rep.OK {
				ok++
				if rep.CommitIndex > maxCommit {
					maxCommit = rep.CommitIndex
				}
				for s, e := range rep.Accepted {
					cur, have := merged[s]
					if !have ||
						e.AcceptNum.Round > cur.AcceptNum.Round ||
						(e.AcceptNum.Round == cur.AcceptNum.Round && e.AcceptNum.NodeID > cur.AcceptNum.NodeID) {
						merged[s] = e
					}
				}
			} else {

			}
		}

		if ok < (len(peers)/2 + 1) {
			n.mu.Lock()
			n.lastPrepareSeen = time.Now()
			n.mu.Unlock()
			continue
		}

		n.mu.Lock()
		n.isLeader = true
		n.LeaderBallot = cand
		n.HighestBallotSeen = cand
		n.LeaderID = myID
		n.LeaderAddr = myAddr

		for s, e := range merged {
			cur, have := n.AcceptLog[s]
			if !have ||
				e.AcceptNum.Round > cur.AcceptNum.Round ||
				(e.AcceptNum.Round == cur.AcceptNum.Round && e.AcceptNum.NodeID > cur.AcceptNum.NodeID) {
				n.AcceptLog[s] = e
				if n.SlotStatus[s] == StatusEmpty {
					n.SlotStatus[s] = StatusAccepted
				}
			}
		}

		maxSeq := maxCommit
		for s := range n.AcceptLog {
			if s > maxSeq {
				maxSeq = s
			}
		}
		if n.CommitIndex < maxCommit {
			n.CommitIndex = maxCommit
		}
		if n.NextSeq <= maxSeq {
			n.NextSeq = maxSeq + 1
		}
		n.lastLeaderMsg = time.Now()
		peers2 := append([]Peer(nil), n.peers...)
		n.mu.Unlock()

		nv := NewViewArgs{
			Ballot:      cand,
			LeaderID:    myID,
			LeaderAddr:  myAddr,
			Accepted:    merged,
			CommitIndex: maxCommit,
		}
		for _, p := range peers2 {
			if p.ID == myID {
				continue
			}
			_ = rpcCall(p.Addr, "Node.NewView", &nv, &NewViewReply{})
		}
	}
}
