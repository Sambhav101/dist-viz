use tokio::sync::mpsc;

use crate::raft::{Envelope, Message, Node};

pub async fn run_cluster(n: u64) {
    let (hub_tx, mut hub_rx) = mpsc::channel::<Envelope>(100);
    let mut node_txs: Vec<mpsc::Sender<Message>> = Vec::new();

    for id in 0..n {
        let (tx, rx) = mpsc::channel::<Message>(100);
        node_txs.push(tx);
        let node = Node::new(id, rx, hub_tx.clone(), n);
        tokio::spawn(node.run());
    }

    while let Some(env) = hub_rx.recv().await {
        println!(" {} -> {} : {:?}", env.from, env.to, env.msg);
        let _ = node_txs[env.to as usize].send(env.msg).await;
    }
}
