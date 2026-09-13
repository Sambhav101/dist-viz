use crate::events::Event;
use tokio::sync::broadcast;

mod events;
mod raft;
mod simulation;
mod ws;

#[tokio::main]
async fn main() {
    let (events, _) = broadcast::channel::<Event>(256);
    tokio::spawn(simulation::run_cluster(5, events.clone()));

    let app = ws::router(events);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    println!("Listening on http://127.0.0.1:3000");
    axum::serve(listener, app).await.unwrap();
}
