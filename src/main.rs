mod events;
mod raft;
mod simulation;

#[tokio::main]
async fn main() {
    simulation::run_cluster(5).await;
}
