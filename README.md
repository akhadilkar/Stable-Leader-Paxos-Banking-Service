# Stable-Leader-Paxos-Banking-Service

1. Built a 5-node Paxos consensus system from scratch in Go that executes 75+ operations across 10 fault scenarios
(crash/leader changes/partial partitions) while preserving linearizability and identical final state across replicas.
2. Implemented timer-based leader election with follower watchdogs and jitter; leader failover completes in 700 ms with
zero safety violations.
3. Added a client redirect queue + retry: requests received during minority/no-leader are safely queued and auto-replayed
after quorum; no duplicates via per-slot/ballot checks.
