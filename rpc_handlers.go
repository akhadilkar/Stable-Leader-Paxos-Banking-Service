package main

import (
	"fmt"
	"sort"
	"time"
)

func (n *Node) Ping(_ *Empty, out *Pong) error {
	out.Msg = fmt.Sprintf("pong from node %d (%s)", n.id, n.addr)
	return nil
}

func (n *Node) GetInfo(_ *Empty, out *Info) error {
	*out = Info{ID: n.id, Addr: n.addr, Peers: n.peers}
	return nil
}

func (n *Node) GetBank(_ *Empty, out *BankReply) error {
	n.mu.Lock()
	if n.failed {
		n.mu.Unlock()
		return fmt.Errorf("node failed")
	}
	n.mu.Unlock()
	out.Accounts = n.printDB()
	return nil
}

func (n *Node) GetStatus(_ *Empty, out *StatusReply) error {
	n.mu.Lock()
	is := n.isLeader
	n.mu.Unlock()
	*out = StatusReply{ID: n.id, IsLeader: is, Addr: n.addr}
	return nil
}
func (n *Node) Accept(a *AcceptArgs, r *AcceptReply) error {
	n.mu.Lock()
	if n.failed {
		n.mu.Unlock()
		return fmt.Errorf("node failed")
	}
	n.mu.Unlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	hb := n.HighestBallotSeen
	if a.Ballot.Round < hb.Round || (a.Ballot.Round == hb.Round && a.Ballot.NodeID < hb.NodeID) {
		r.OK = false
		r.HighestBallot = hb
		return nil
	}

	if a.Ballot.Round > hb.Round || (a.Ballot.Round == hb.Round && a.Ballot.NodeID > hb.NodeID) {
		n.HighestBallotSeen = a.Ballot
	}

	if a.Ballot.Round > n.HighestBallotSeen.Round ||
		(a.Ballot.Round == n.HighestBallotSeen.Round && a.Ballot.NodeID > n.HighestBallotSeen.NodeID) {
		n.HighestBallotSeen = a.Ballot
	}
	n.LeaderID = a.Ballot.NodeID
	n.lastLeaderMsg = time.Now()

	st := n.SlotStatus[a.Seq]
	if st == StatusCommitted || st == StatusExecuted {
		r.OK = true
		return nil
	}

	n.AcceptLog[a.Seq] = AcceptLogEntry{
		AcceptNum: a.Ballot,
		AcceptSeq: a.Seq,
		AcceptVal: a.Req,
	}
	n.SlotStatus[a.Seq] = StatusAccepted
	r.OK = true
	return nil
}

func (n *Node) Submit(in *SubmitArgs, out *SubmitReply) error {

	n.mu.Lock()
	if n.failed {
		n.mu.Unlock()
		return fmt.Errorf("node failed")
	}
	n.mu.Unlock()
	n.mu.Lock()
	last := n.LastTau[in.C]
	if in.Tau <= last {
		rep := n.LastReply[in.C]
		n.mu.Unlock()
		out.OK, out.Msg = rep.OK, rep.Msg
		return nil
	}
	n.mu.Unlock()

	n.mu.Lock()
	isLeader := n.isLeader
	lb := n.LeaderBallot
	n.mu.Unlock()

	if !isLeader {
		out.OK = false
		out.Msg = "redirect"
		n.mu.Lock()
		out.Redirect = n.LeaderAddr
		n.mu.Unlock()
		if out.Redirect == "" {
			out.Redirect = discoverLeaderAddr(n)
		}
		return nil
	}

	req := Request{
		T:   Transaction{From: in.From, To: in.To, Amt: in.Amt},
		Tau: in.Tau,
		C:   in.C,
	}

	n.mu.Lock()
	seq := n.NextSeq
	n.NextSeq++
	n.AcceptLog[seq] = AcceptLogEntry{AcceptNum: lb, AcceptSeq: seq, AcceptVal: req}
	n.SlotStatus[seq] = StatusAccepted
	n.mu.Unlock()

	need := len(n.peers)/2 + 1
	okCount := 1
	for _, p := range n.peers {
		if p.ID == n.id {
			continue
		}
		var rep AcceptReply
		if err := rpcCall(p.Addr, "Node.Accept",
			&AcceptArgs{Ballot: lb, Seq: seq, Req: req}, &rep); err == nil && rep.OK {
			okCount++
		}
	}

	if okCount < need {

		n.mu.Lock()

		already := false
		for _, s := range n.PendingSeqs {
			if s == seq {
				already = true
				break
			}
		}
		if !already {
			n.PendingSeqs = append(n.PendingSeqs, seq)
		}
		n.mu.Unlock()

		out.OK = false
		out.Msg = fmt.Sprintf("queued (no majority) at slot %d (ok=%d need=%d)", seq, okCount, need)
		return nil
	}

	for _, p := range n.peers {
		if p.ID == n.id {
			continue
		}
		_ = rpcCall(p.Addr, "Node.Commit", &CommitArgs{Ballot: lb, Seq: seq, Req: req}, &CommitReply{})
	}
	_ = n.Commit(&CommitArgs{Ballot: lb, Seq: seq, Req: req}, &CommitReply{})

	n.mu.Lock()
	rep := n.LastReply[in.C]
	n.mu.Unlock()
	out.OK, out.Msg = rep.OK, rep.Msg

	return nil
}

func (n *Node) Commit(c *CommitArgs, r *CommitReply) error {
	n.mu.Lock()
	if n.failed {
		n.mu.Unlock()
		return fmt.Errorf("node failed")
	}
	n.mu.Unlock()
	n.mu.Lock()

	if c.Ballot.Round > n.HighestBallotSeen.Round ||
		(c.Ballot.Round == n.HighestBallotSeen.Round && c.Ballot.NodeID > n.HighestBallotSeen.NodeID) {
		n.HighestBallotSeen = c.Ballot
	}
	n.LeaderID = c.Ballot.NodeID
	n.lastLeaderMsg = time.Now()

	defer n.mu.Unlock()

	if _, ok := n.AcceptLog[c.Seq]; !ok {
		n.AcceptLog[c.Seq] = AcceptLogEntry{
			AcceptNum: c.Ballot,
			AcceptSeq: c.Seq,
			AcceptVal: c.Req,
		}
	}
	if n.SlotStatus[c.Seq] < StatusCommitted {
		n.SlotStatus[c.Seq] = StatusCommitted
	}
	if c.Seq > n.CommitIndex {
		n.CommitIndex = c.Seq
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

	r.OK = true
	return nil
}
func (n *Node) GetCommitted(a *FetchArgs, r *FetchReply) error {
	n.mu.Lock()
	if n.failed {
		n.mu.Unlock()
		return fmt.Errorf("node failed")
	}
	n.mu.Unlock()
	n.mu.Lock()
	from := a.From
	if from < 1 {
		from = 1
	}
	to := n.CommitIndex
	ents := make([]AcceptLogEntry, 0, to-from+1)
	for s := from; s <= to; s++ {
		if e, ok := n.AcceptLog[s]; ok {
			ents = append(ents, e)
		}
	}
	r.Entries = ents
	r.CommitIndex = n.CommitIndex
	n.mu.Unlock()
	return nil
}

func (n *Node) SetFailed(a *FailArgs, _ *Empty) error {
	n.mu.Lock()
	was := n.failed
	n.failed = a.Failed
	if a.Failed {
		n.isLeader = false
	}
	n.mu.Unlock()

	if was && !a.Failed {

		//go n.catchUp()
		n.catchUp()
	}
	return nil
}

func (n *Node) SetLeader(a *LeaderArgs, _ *Empty) error {
	n.mu.Lock()
	if a.On {
		n.isLeader = true

		r := n.HighestBallotSeen.Round + 1
		n.LeaderBallot = Ballot{Round: r, NodeID: n.id}
		n.HighestBallotSeen = n.LeaderBallot

		if n.NextSeq <= n.CommitIndex {
			n.NextSeq = n.CommitIndex + 1
		}

		go func() {
			for i := 0; i < 5; i++ {
				var rr RetryReply
				_ = n.RetryPending(&Empty{}, &rr)
				if rr.Count == 0 {
					break
				}
				time.Sleep(80 * time.Millisecond)
			}
		}()

	} else {
		n.isLeader = false
	}
	n.mu.Unlock()

	if a.On {
		go n.catchUp()
	}
	return nil
}

func (n *Node) GetLog(_ *Empty, r *LogReply) error {
	n.mu.Lock()
	if n.failed {
		n.mu.Unlock()
		return fmt.Errorf("node failed")
	}
	n.mu.Unlock()
	n.mu.Lock()
	defer n.mu.Unlock()

	ks := make([]int, 0, len(n.AcceptLog))
	for s := range n.AcceptLog {
		ks = append(ks, s)
	}
	sort.Ints(ks)

	entries := make([]LogEntryView, 0, len(ks))
	status := func(st SlotStatus) string {
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
	for _, s := range ks {
		e := n.AcceptLog[s]
		entries = append(entries, LogEntryView{
			Seq:    s,
			Status: status(n.SlotStatus[s]),
			From:   e.AcceptVal.T.From,
			To:     e.AcceptVal.T.To,
			Amt:    e.AcceptVal.T.Amt,
			Ballot: e.AcceptNum,
		})
	}
	r.Entries = entries
	return nil
}
func (n *Node) RetryPending(_ *Empty, out *RetryReply) error {
	n.mu.Lock()
	if !n.isLeader {
		n.mu.Unlock()
		out.Count = 0
		return nil
	}

	seqs := append([]int(nil), n.PendingSeqs...)
	lb := n.LeaderBallot
	n.mu.Unlock()

	committed := 0
	need := len(n.peers)/2 + 1

	for _, seq := range seqs {

		n.mu.Lock()
		e, ok := n.AcceptLog[seq]
		n.mu.Unlock()
		if !ok {

			continue
		}
		req := e.AcceptVal

		okCount := 1
		for _, p := range n.peers {
			if p.ID == n.id {
				continue
			}
			var rep AcceptReply
			if err := rpcCall(p.Addr, "Node.Accept",
				&AcceptArgs{Ballot: lb, Seq: seq, Req: req}, &rep); err == nil && rep.OK {
				okCount++
			}
		}
		if okCount < need {
			continue
		}

		for _, p := range n.peers {
			if p.ID == n.id {
				continue
			}
			_ = rpcCall(p.Addr, "Node.Commit",
				&CommitArgs{Ballot: lb, Seq: seq, Req: req}, &CommitReply{})
		}
		_ = n.Commit(&CommitArgs{Ballot: lb, Seq: seq, Req: req}, &CommitReply{})
		committed++

		n.mu.Lock()
		dst := n.PendingSeqs[:0]
		for _, s := range n.PendingSeqs {
			if s != seq {
				dst = append(dst, s)
			}
		}
		n.PendingSeqs = dst
		n.mu.Unlock()
	}
	out.Count = committed
	return nil
}
func (n *Node) Prepare(a *PrepareArgs, r *PrepareReply) error {
	n.mu.Lock()
	if n.failed {
		n.mu.Unlock()
		return fmt.Errorf("node failed")
	}
	n.lastPrepareSeen = time.Now()

	hb := n.HighestBallotSeen

	if a.Ballot.Round < hb.Round || (a.Ballot.Round == hb.Round && a.Ballot.NodeID < hb.NodeID) {
		n.mu.Unlock()
		r.OK = false
		r.HighestBallot = hb
		return nil
	}

	n.HighestBallotSeen = a.Ballot
	n.isLeader = false
	n.LeaderID, n.LeaderAddr = 0, ""
	acc := make(map[int]AcceptLogEntry, len(n.AcceptLog))
	for s, e := range n.AcceptLog {
		acc[s] = e
	}
	ci := n.CommitIndex
	n.mu.Unlock()

	r.OK = true
	r.HighestBallot = a.Ballot
	r.Accepted = acc
	r.CommitIndex = ci
	return nil
}

func (n *Node) NewView(a *NewViewArgs, r *NewViewReply) error {
	n.mu.Lock()
	if n.failed {
		n.mu.Unlock()
		return fmt.Errorf("node failed")
	}
	hb := n.HighestBallotSeen

	if a.Ballot.Round < hb.Round || (a.Ballot.Round == hb.Round && a.Ballot.NodeID < hb.NodeID) {
		n.mu.Unlock()
		r.OK = false
		return nil
	}

	n.HighestBallotSeen = a.Ballot
	n.isLeader = false
	n.LeaderID, n.LeaderAddr = a.LeaderID, a.LeaderAddr

	for s, e := range a.Accepted {
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
	if n.CommitIndex < a.CommitIndex {
		n.CommitIndex = a.CommitIndex
	}

	maxSeq := n.CommitIndex
	for s := range n.AcceptLog {
		if s > maxSeq {
			maxSeq = s
		}
	}
	if n.NextSeq <= maxSeq {
		n.NextSeq = maxSeq + 1
	}
	n.lastLeaderMsg = time.Now()
	n.mu.Unlock()

	r.OK = true
	return nil
}
