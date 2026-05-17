use tokio::sync::mpsc;

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

#[derive(Debug)]
struct Node {
	id: u64,
	state: NodeState,
	current_term: u64,
	voted_for: Option<u64>,
	votes_received: u64,
	rx: mpsc::Receiver<Message>,
}
