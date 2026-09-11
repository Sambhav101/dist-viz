use tokio::sync::mpsc;

#[derive(Debug)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub enum Message {
    RequestVote { from: u64, term: u64 },
    RequestVoteReply { from: u64, term: u64, granted: bool },
    AppendEntries { from: u64, term: u64 },
    AppendEntriesReply { from: u64, term: u64, success: bool },
}

#[derive(Debug)]
pub struct Node {
    id: u64,
    state: NodeState,
    current_term: u64,
    voted_for: Option<u64>,
    votes_received: u64,
    rx: mpsc::Receiver<Message>,
    tx: mpsc::Sender<Envelope>,
    cluster_size: u64,
}

#[derive(Debug)]
pub struct Envelope {
    pub from: u64,
    pub to: u64,
    pub msg: Message,
}

impl Node {
    // constructor for our Node class
    fn new(
        id: u64,
        rx: mpsc::Receiver<Message>,
        tx: mpsc::Sender<Envelope>,
        cluster_size: u64,
    ) -> Self {
        Node {
            id,
            state: NodeState::Follower,
            current_term: 0,
            voted_for: None,
            votes_received: 0,
            rx,
            tx,
            cluster_size,
        }
    }

    // a candidate will start election and votes for itself
    async fn start_election(&mut self) {
        self.state = NodeState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.votes_received = 1;

        for peer in 0..self.cluster_size {
            if peer == self.id {
                continue;
            }
            let req = Message::RequestVote {
                from: self.id,
                term: self.current_term,
            };
            self.send_to(peer, req).await;
        }
    }

    // when a node receives a requestVote, decide to grant the vote or not
    async fn handle_message(&mut self, msg: Message) {
        // any message from higher term means we are stale
        if msg.term() > self.current_term {
            self.current_term = msg.term();
            self.state = NodeState::Follower;
            self.voted_for = None;
        }

        match msg {
            // if a node is requesting vote, update voted for if conditions met
            Message::RequestVote { from, term } => {
                let granted = term == self.current_term
                    && (self.voted_for.is_none() || self.voted_for == Some(from));
                if granted {
                    self.voted_for = Some(from);
                }
                let reply = Message::RequestVoteReply {
                    from: self.id,
                    term: self.current_term,
                    granted,
                };
                self.send_to(from, reply).await;
            }
            Message::RequestVoteReply { granted, .. } => {
                if granted {
                    self.votes_received += 1;
                    if self.votes_received > self.cluster_size / 2 {
                        self.state = NodeState::Leader;
                    }
                }
            }
            // catch-all for other vairants that we didn't includ
            _ => {}
        }
    }

    async fn send_to(&self, to: u64, msg: Message) {
        let env = Envelope {
            from: self.id,
            to,
            msg,
        };
        let _ = self.tx.send(env).await;
    }
}

impl Message {
    pub fn term(&self) -> u64 {
        match self {
            Message::RequestVote { term, .. }
            | Message::RequestVoteReply { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesReply { term, .. } => *term,
        }
    }
}
