module raftkv

go 1.15

replace (
	labgob => ../labgob
	labrpc => ../labrpc
	raft => ../raft
	linearizability => ../linearizability
)

require (
	labgob v0.0.0-00010101000000-000000000000
	labrpc v0.0.0-00010101000000-000000000000
	raft v0.0.0-00010101000000-000000000000
	linearizability v0.0.0-00010101000000-000000000000
)
