use crate::events::Event;
use tokio::sync::broadcast;

mod events;
mod raft;
mod simulation;

#[tokio::main]
async fn main() {
    let (events, _) = broadcast::channel::<Event>(256);
    simulation::run_cluster(5, events).await;
}
