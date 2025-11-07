package main

import (
	"net"
	"net/rpc"
	"time"
)

func rpcCall(addr, method string, in, out interface{}) error {
	conn, err := net.DialTimeout("tcp", addr, 800*time.Millisecond)
	if err != nil {
		return err
	}
	cli := rpc.NewClient(conn)
	defer cli.Close()
	return cli.Call(method, in, out)
}

func addrOfPeer(n *Node, id int) (string, bool) {
	for _, p := range n.peers {
		if p.ID == id {
			return p.Addr, true
		}
	}
	return "", false
}

func discoverLeaderAddr(n *Node) string {
	for _, p := range n.peers {
		var s StatusReply
		if err := rpcCall(p.Addr, "Node.GetStatus", &Empty{}, &s); err == nil && s.IsLeader {
			return s.Addr
		}
	}
	return ""
}

func discoverLeaderAddrFromPeers(peers []Peer) string {
	for _, p := range peers {
		var s StatusReply
		if err := rpcCall(p.Addr, "Node.GetStatus", &Empty{}, &s); err == nil && s.IsLeader {
			return s.Addr
		}
	}
	return ""
}
