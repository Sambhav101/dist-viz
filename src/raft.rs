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
	// constructor for our Node class
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
	
	// a candidate will start election and votes for itself
	fn start_election(&mut self) {
		self.state = NodeState::Candidate;
		self.current_term += 1;
		self.voted_for = Some(self.id);
		self.votes_received = 1;
	}
	
	// when a node receives a requestVote, decide to grant the vote or not
	fn handle_message(&mut self, msg: Message) {
		match msg {
			// if a node is requesting vote, update voted for if conditions met
			Message::RequestVote { from, term } => {
				if term >= self.current_term && ( self.voted_for.is_none() || self.voted_for == Some(from) ) {
					self.voted_for = Some(from);
				}
			}
			// if a node is replying to request vote, increment its vote by 1
			Message::RequestVoteReply {granted, .. } => {
				if granted {
					self.votes_received += 1;
				}	
			}
			// catch-all for other vairants that we didn't includ
			_ => {}
		}	
	}
}
