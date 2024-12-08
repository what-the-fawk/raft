package common

type ConsensusState int

const (
	StateFollower ConsensusState = iota
	StateCandidate
	StateLeader
)
