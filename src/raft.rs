#[derive(Debug)]
enum NodeState {
	Follower,
	Candidate,
	Leader,
}

#[derive(Debug)]
enum Message {
	RequestVote { from: u64, term: u64},
	RequestVoteReply { from: u64, term: u64, granted: bool},
	AppendEntries { from: u64, term: u64},
	AppendEntriesReply { from: u64, term: u64, success: bool},
}
