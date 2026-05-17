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

impl Node {
	fn new(id: u64, rx: mpsc::Receiver<Message>) -> Self {
		Node {
			id,
			state: NodeState::Follower,
			current_term: 0,
			voted_for: None,
			votes_received: 0,
			rx,
		}
	}
	
	fn start_election(&mut self) {
		self.state = NodeState::Candidate;
		self.current_term += 1;
		self.voted_for = Some(self.id);
		self.votes_received = 1;
	}
}
