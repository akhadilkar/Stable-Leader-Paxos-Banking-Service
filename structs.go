package main

import (
	"sync"
	"time"
)

type Peer struct {
	ID   int
	Addr string
}

type Node struct {
	id    int
	addr  string
	peers []Peer

	mu       sync.Mutex
	bank     map[string]int64
	isLeader bool

	failed bool

	HighestBallotSeen Ballot
	LeaderBallot      Ballot
	NextSeq           int
	CommitIndex       int
	ExecuteIndex      int
	AcceptLog         map[int]AcceptLogEntry
	SlotStatus        map[int]SlotStatus

	LastReply map[ClientID]ReplyArgs
	LastTau   map[ClientID]int64

	PendingSeqs []int

	LeaderID        int
	LeaderAddr      string
	lastLeaderMsg   time.Time
	lastPrepareSeen time.Time
}

type Empty struct{}
type Pong struct{ Msg string }

type Info struct {
	ID    int
	Addr  string
	Peers []Peer
}

type BankReply struct {
	Accounts map[string]int64
}

type StatusReply struct {
	ID       int
	IsLeader bool
	Addr     string
}

type Ballot struct {
	Round  int
	NodeID int
}
type ClientID string

type Transaction struct {
	From, To string
	Amt      int64
}
type Request struct {
	T   Transaction
	Tau int64
	C   ClientID
}

type AcceptLogEntry struct {
	AcceptNum  Ballot
	AcceptSeq  int
	AcceptVal  Request
	Failed     bool
	FailReason string
}
type SlotStatus int

const (
	StatusEmpty SlotStatus = iota
	StatusProposed
	StatusAccepted
	StatusCommitted
	StatusExecuted
)

type PrepareArgs struct {
	Ballot        Ballot
	CandidateID   int
	CandidateAddr string
}
type PrepareReply struct {
	OK            bool
	HighestBallot Ballot
	Accepted      map[int]AcceptLogEntry
	CommitIndex   int
}
type NewViewArgs struct {
	Ballot      Ballot
	LeaderID    int
	LeaderAddr  string
	Accepted    map[int]AcceptLogEntry
	CommitIndex int
}
type NewViewReply struct{ OK bool }

type AcceptArgs struct {
	Ballot Ballot
	Seq    int
	Req    Request
}
type AcceptReply struct {
	OK            bool
	HighestBallot Ballot
}

type CommitArgs struct {
	Ballot Ballot
	Seq    int
	Req    Request
}
type CommitReply struct{ OK bool }

type SubmitArgs struct {
	From, To string
	Amt      int64
	C        ClientID
	Tau      int64
}
type ReplyArgs struct {
	OK  bool
	Msg string
}
type SubmitReply struct {
	OK       bool
	Msg      string
	Redirect string
}

type FetchArgs struct {
	From int
}
type FetchReply struct {
	Entries     []AcceptLogEntry
	CommitIndex int
}
type FailArgs struct{ Failed bool }
type LeaderArgs struct{ On bool }

type LogEntryView struct {
	Seq    int
	Status string
	From   string
	To     string
	Amt    int64
	Ballot Ballot
}
type LogReply struct {
	Entries []LogEntryView
}
type RetryReply struct{ Count int }

type Tx struct {
	From string
	To   string
	Amt  int64

	Client ClientID
	Tau    int64
}

type LogEntry struct {
	Slot   int
	Ballot Ballot
	Cmd    Tx
	Status SlotStatus
}
